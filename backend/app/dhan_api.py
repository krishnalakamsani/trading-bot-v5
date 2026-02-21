# Dhan API wrapper
try:
    from dhanhq import dhanhq  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    dhanhq = None
from datetime import datetime, timezone, timedelta
import logging
from config import bot_state
from indices import get_index_config

logger = logging.getLogger(__name__)
DEFAULT_FNO_SEGMENT = "NSE_FNO"

class DhanAPI:
    def __init__(self, access_token: str, client_id: str):
        if dhanhq is None:
            raise RuntimeError(
                "Dhan SDK not installed (missing 'dhanhq'). Install it to use live trading/quotes."
            )
        self.access_token = access_token
        self.client_id = client_id
        self.dhan = dhanhq(client_id, access_token)
        self._default_exchange_segment = getattr(self.dhan, DEFAULT_FNO_SEGMENT, None)
        self._segment_ready = self._default_exchange_segment is not None
        if self._default_exchange_segment is None:
            logger.error(
                f"[ORDER] Dhan API missing segment attribute: {DEFAULT_FNO_SEGMENT}. "
                "Verify the Dhan SDK version and initialization; order placement will fail."
            )
        # Cache for option chain to avoid rate limiting
        self._option_chain_cache = {}
        self._option_chain_cache_time = {}
        self._cache_duration = 60  # Default cache for 60 seconds
        self._position_cache_duration = 10  # Shorter cache when position is open

    def _extract_option_chain_oc(self, chain: dict) -> object:
        """Extract option-chain 'oc' payload from Dhan response.

        Dhan payload shape has varied historically between:
        - {'status': 'success', 'data': {'data': {'oc': {...}}}}
        - {'status': 'success', 'data': {'oc': {...}}}
        """
        if not chain or chain.get('status') != 'success':
            return {}

        data = chain.get('data', {})
        if isinstance(data, dict) and 'data' in data and isinstance(data.get('data'), dict):
            data = data.get('data', {})

        if isinstance(data, dict):
            return data.get('oc', {})
        return {}

    def _match_strike_node(self, oc_data: object, strike: int) -> tuple:
        """Return (matched_key, strike_node_dict) for a given strike.

        Handles oc_data as either dict keyed by strike (stringified floats) or
        a list of strike entries.
        """
        if not oc_data:
            return None, None

        # Dict form: {'25100.000000': {'ce': {...}, 'pe': {...}}, ...}
        if isinstance(oc_data, dict):
            # Fast path for common exact key formats
            candidate_keys = [
                f"{strike}.000000",
                f"{strike}.0000",
                f"{strike}.00",
                f"{strike}.0",
                str(strike),
            ]
            for key in candidate_keys:
                node = oc_data.get(key)
                if isinstance(node, dict) and node:
                    return key, node

            # Robust numeric match (handles weird decimal formatting)
            best_key = None
            best_node = None
            best_diff = None
            for key, node in oc_data.items():
                if not isinstance(node, dict):
                    continue
                try:
                    numeric_key = float(str(key))
                except Exception:
                    continue
                diff = abs(numeric_key - float(strike))
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_key = key
                    best_node = node

            if best_diff is not None and best_diff < 0.001:
                return str(best_key), best_node

            return None, None

        # List form: [{'strike_price': 25100, 'ce': {...}}, ...]
        if isinstance(oc_data, list):
            for entry in oc_data:
                if not isinstance(entry, dict):
                    continue
                sp = entry.get('strike_price')
                if sp is None:
                    sp = entry.get('strikePrice')
                if sp is None:
                    sp = entry.get('strike')
                try:
                    if sp is not None and abs(float(sp) - float(strike)) < 0.001:
                        return str(sp), entry
                except Exception:
                    continue
            return None, None

        return None, None

    def _match_nearest_strike_node(self, oc_data: object, strike: int, max_diff: float) -> tuple:
        """Return the nearest (matched_key, strike_node_dict) within max_diff.

        This is used as a fallback when the exact strike key is not present in the option chain,
        which can happen when strike intervals differ (e.g., SENSEX) or when the chain omits
        certain strikes.
        """
        if not oc_data:
            return None, None

        best_key = None
        best_node = None
        best_diff = None

        if isinstance(oc_data, dict):
            for key, node in oc_data.items():
                if not isinstance(node, dict) or not node:
                    continue
                try:
                    numeric_key = float(str(key))
                except Exception:
                    continue
                diff = abs(numeric_key - float(strike))
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_key = key
                    best_node = node

        elif isinstance(oc_data, list):
            for entry in oc_data:
                if not isinstance(entry, dict):
                    continue
                sp = entry.get('strike_price')
                if sp is None:
                    sp = entry.get('strikePrice')
                if sp is None:
                    sp = entry.get('strike')
                try:
                    numeric_sp = float(sp)
                except Exception:
                    continue
                diff = abs(numeric_sp - float(strike))
                if best_diff is None or diff < best_diff:
                    best_diff = diff
                    best_key = str(sp)
                    best_node = entry

        if best_diff is not None and best_diff <= float(max_diff):
            return str(best_key), best_node
        return None, None

    def _extract_security_id(self, opt_data: object) -> str:
        """Extract security id from option payload across possible key names."""
        if not isinstance(opt_data, dict):
            return ""
        security_id = opt_data.get('security_id')
        if security_id is None:
            security_id = opt_data.get('securityId')
        if security_id is None and isinstance(opt_data.get('instrument'), dict):
            security_id = opt_data['instrument'].get('security_id')
        return str(security_id) if security_id else ""
    
    def get_index_ltp(self, index_name: str = "NIFTY") -> float:
        """Get index spot LTP"""
        try:
            index_config = get_index_config(index_name)
            security_id = index_config["security_id"]
            segment = index_config["exchange_segment"]
            
            # For SENSEX, try multiple segments as Dhan API may vary
            segments_to_try = [segment]
            if index_name == "SENSEX":
                segments_to_try = ["IDX_I", "BSE_INDEX", "BSE"]
            
            for seg in segments_to_try:
                response = self.dhan.quote_data({
                    seg: [security_id]
                })
                
                if response and response.get('status') == 'success':
                    data = response.get('data', {})
                    if isinstance(data, dict) and 'data' in data:
                        data = data.get('data', {})
                    
                    idx_data = data.get(seg, {}).get(str(security_id), {})
                    if idx_data:
                        ltp = idx_data.get('last_price')
                        if ltp and ltp > 0:
                            logger.debug(f"Got {index_name} LTP: {ltp} from segment {seg}")
                            logger.info(f"[TICK] Dhan get_index_ltp -> {index_name}: {ltp} (segment={seg})")
                            return float(ltp)
                        ohlc = idx_data.get('ohlc', {})
                        if ohlc and ohlc.get('close'):
                            return float(ohlc.get('close'))
                    
        except Exception as e:
            logger.error(f"Error fetching {index_name} LTP: {e}")
        return 0
    
    def get_index_and_option_ltp(self, index_name: str, option_security_id: int) -> tuple:
        """Get both Index and Option LTP in a single API call"""
        index_ltp = 0
        option_ltp = 0
        
        try:
            index_config = get_index_config(index_name)
            security_id = index_config["security_id"]
            segment = index_config["exchange_segment"]
            fno_segment = index_config.get("fno_segment", "NSE_FNO")
            
            # Fetch both in single call to avoid rate limits
            response = self.dhan.quote_data({
                segment: [security_id],
                fno_segment: [option_security_id]
            })
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                if isinstance(data, dict) and 'data' in data:
                    data = data.get('data', {})
                
                # Get Index LTP
                idx_data = data.get(segment, {}).get(str(security_id), {})
                if idx_data:
                    try:
                        index_ltp = float(idx_data.get('last_price', 0))
                    except Exception:
                        index_ltp = 0
                
                # Get Option LTP
                fno_data = data.get(fno_segment, {}).get(str(option_security_id), {})
                if fno_data:
                    try:
                        option_ltp = float(fno_data.get('last_price', 0))
                    except Exception:
                        option_ltp = 0

                logger.info(f"[TICK] Quote: {index_name}={index_ltp}, Option {option_security_id}={option_ltp} (segments: index={segment}, option={fno_segment})")
                    
        except Exception as e:
            logger.error(f"Error fetching combined quote: {e}")
        
        return index_ltp, option_ltp
    
    async def get_option_chain(self, index_name: str = "NIFTY", expiry: str = None, force_refresh: bool = False) -> dict:
        """Get option chain with caching"""
        try:
            import asyncio
            index_config = get_index_config(index_name)
            security_id = index_config["security_id"]
            
            if not expiry:
                expiry = await self.get_nearest_expiry(index_name)
            
            if not expiry:
                logger.error("Could not determine expiry date")
                return {}
            
            # Check cache
            cache_key = f"{index_name}_{expiry}"
            now = datetime.now()
            
            cache_duration = self._position_cache_duration if bot_state.get('current_position') else self._cache_duration
            
            cache_time = self._option_chain_cache_time.get(cache_key)
            if (not force_refresh and 
                self._option_chain_cache.get(cache_key) and 
                cache_time and 
                (now - cache_time).seconds < cache_duration):
                return self._option_chain_cache[cache_key]
            
            logger.info(f"Fetching fresh option chain: {index_name}, expiry={expiry}")
            
            response = await asyncio.to_thread(
                self.dhan.option_chain,
                under_security_id=security_id,
                under_exchange_segment='IDX_I',
                expiry=expiry
            )
            
            if response and response.get('status') == 'success':
                self._option_chain_cache[cache_key] = response
                self._option_chain_cache_time[cache_key] = now
                logger.info(f"Option chain cached at {now.strftime('%H:%M:%S')}")
            
            return response if response else {}
        except Exception as e:
            logger.error(f"Error fetching option chain: {e}")
        return {}
    
    async def get_nearest_expiry(self, index_name: str = "NIFTY") -> str:
        """Get nearest expiry date"""
        try:
            import asyncio
            index_config = get_index_config(index_name)
            security_id = index_config["security_id"]
            
            for segment in ['IDX_I', 'NSE_FNO', 'INDEX']:
                logger.info(f"Trying expiry_list for {index_name} with segment: {segment}")
                response = await asyncio.to_thread(
                    self.dhan.expiry_list,
                    under_security_id=security_id,
                    under_exchange_segment=segment
                )
                logger.info(f"Expiry list response: {response}")
                
                if response and response.get('status') == 'success':
                    data = response.get('data', {})
                    if isinstance(data, dict) and 'data' in data:
                        expiries = data.get('data', [])
                    elif isinstance(data, list):
                        expiries = data
                    else:
                        expiries = []
                    
                    if expiries and isinstance(expiries, list):
                        today = datetime.now().date()
                        
                        valid_expiries = []
                        for exp in expiries:
                            try:
                                if isinstance(exp, str):
                                    if '-' in exp:
                                        exp_date = datetime.strptime(exp, "%Y-%m-%d").date()
                                    elif '/' in exp:
                                        exp_date = datetime.strptime(exp, "%d/%m/%Y").date()
                                    else:
                                        continue
                                    
                                    if exp_date >= today:
                                        valid_expiries.append((exp_date, exp))
                            except ValueError:
                                continue
                        
                        if valid_expiries:
                            valid_expiries.sort(key=lambda x: x[0])
                            nearest = valid_expiries[0][1]
                            logger.info(f"Nearest expiry for {index_name}: {nearest}")
                            return nearest
            
            logger.warning(f"Could not get expiry list from API for {index_name}")
        except Exception as e:
            logger.error(f"Error getting expiry list: {e}")
        
        # Fallback: calculate based on index expiry day
        index_config = get_index_config(index_name)
        expiry_day = index_config["expiry_day"]
        
        ist = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)
        days_until_expiry = (expiry_day - ist.weekday()) % 7
        if days_until_expiry == 0:
            if ist.hour >= 15 and ist.minute >= 30:
                days_until_expiry = 7
        expiry_date = ist + timedelta(days=days_until_expiry)
        calculated_expiry = expiry_date.strftime("%Y-%m-%d")
        logger.info(f"Using calculated expiry for {index_name}: {calculated_expiry}")
        return calculated_expiry
    
    async def get_atm_option_security_id(self, index_name: str, strike: int, option_type: str, expiry: str = None) -> str:
        """Get security ID for ATM option"""
        try:
            if not expiry:
                expiry = await self.get_nearest_expiry(index_name)

            index_config = get_index_config(index_name)
            strike_interval = float(index_config.get('strike_interval', 0) or 0)
            # Fallback tolerance for nearest strike matching (helps SENSEX/BSE chains).
            # Allow up to 1 interval difference; if interval is unknown, allow 100.
            nearest_max_diff = strike_interval if strike_interval > 0 else 100.0

            # Dhan option-chain can intermittently return partial/empty oc payload.
            # Retry once with force_refresh to avoid cache + transient API hiccups.
            import asyncio
            for attempt in range(2):
                chain = await self.get_option_chain(index_name=index_name, expiry=expiry, force_refresh=(attempt == 1))

                if not chain or chain.get('status') != 'success':
                    if attempt == 0:
                        await asyncio.sleep(0.25)
                        continue
                    break

                oc_data = self._extract_option_chain_oc(chain)
                if not oc_data:
                    logger.warning(f"Option chain missing/empty oc for {index_name} expiry={expiry} (attempt {attempt + 1}/2)")
                    if attempt == 0:
                        await asyncio.sleep(0.25)
                        continue
                    break

                matched_key, strike_node = self._match_strike_node(oc_data, strike)
                if not strike_node:
                    matched_key, strike_node = self._match_nearest_strike_node(oc_data, strike, max_diff=nearest_max_diff)
                if isinstance(strike_node, dict) and strike_node:
                    # Dhan payload keys can vary in case; try a small set of common keys.
                    if option_type.upper() == 'CE':
                        opt_candidates = ['ce', 'CE', 'call', 'CALL']
                    else:
                        opt_candidates = ['pe', 'PE', 'put', 'PUT']

                    opt_data = {}
                    for k in opt_candidates:
                        if isinstance(strike_node.get(k), dict) and strike_node.get(k):
                            opt_data = strike_node.get(k)
                            break

                    security_id = self._extract_security_id(opt_data)
                    if security_id:
                        logger.info(
                            f"Found security ID {security_id} for {index_name} {strike} {option_type} (key={matched_key}, attempt {attempt + 1}/2)"
                        )
                        return security_id

                if attempt == 0:
                    await asyncio.sleep(0.25)
                    continue

                # Helpful diagnostics after final attempt
                if isinstance(oc_data, dict):
                    available_strikes = list(oc_data.keys())[:10]
                elif isinstance(oc_data, list):
                    available_strikes = [
                        (x.get('strike_price') or x.get('strikePrice') or x.get('strike'))
                        for x in oc_data[:10] if isinstance(x, dict)
                    ]
                else:
                    available_strikes = []
                logger.warning(
                    f"Strike {strike} not found in option chain for {index_name} expiry={expiry} after retry. Sample strikes: {available_strikes}"
                )
            
            logger.warning(
                f"Could not find security ID for {index_name} {strike} {option_type}. "
                "This usually means the option chain does not contain that strike/side or the underlying security_id/segment is incorrect."
            )
        except Exception as e:
            logger.error(f"Error getting ATM option security ID: {e}")
        return ""
    
    async def get_option_ltp(self, security_id: str, strike: int = None, option_type: str = None, expiry: str = None, index_name: str = "NIFTY") -> float:
        """Get option LTP from cache or API"""
        try:
            index_config = get_index_config(index_name)
            fno_segment = index_config.get("fno_segment", "NSE_FNO")
            
            # First try from cached option chain
            if strike and option_type:
                cache_key = f"{index_name}_{expiry}" if expiry else None
                if cache_key and self._option_chain_cache.get(cache_key):
                    chain = self._option_chain_cache[cache_key]
                    oc_data = self._extract_option_chain_oc(chain)
                    _, strike_node = self._match_strike_node(oc_data, strike)

                    if isinstance(strike_node, dict) and strike_node:
                        opt_key = 'ce' if option_type.upper() == 'CE' else 'pe'
                        opt_data = strike_node.get(opt_key, {})
                        if isinstance(opt_data, dict):
                            ltp = opt_data.get('last_price', 0) or opt_data.get('lastPrice', 0)
                            if ltp and float(ltp) > 0:
                                logger.info(f"Got option LTP from cache: {index_name} {strike} {option_type} = {ltp}")
                                return float(ltp)
            
            # Fallback: Make API call
            logger.info(f"Fetching option LTP for security_id: {security_id}")
            response = self.dhan.quote_data({
                fno_segment: [int(security_id)]
            })
            
            if response and response.get('status') == 'success':
                data = response.get('data', {})
                if isinstance(data, dict) and 'data' in data:
                    data = data.get('data', {})
                
                fno_data = data.get(fno_segment, {}).get(str(security_id), {})
                if fno_data:
                    ltp = fno_data.get('last_price')
                    if ltp and ltp > 0:
                        return float(ltp)
                        
        except Exception as e:
            logger.error(f"Error fetching option LTP: {e}")
        return 0
    
    async def place_order(self, security_id: str, transaction_type: str, qty: int, index_name: str = None) -> dict:
        """Place a market order synchronously (Dhan API is synchronous)"""
        try:
            import asyncio
            if not self._segment_ready:
                return {
                    "status": "error",
                    "message": f"Dhan API missing segment attribute: {DEFAULT_FNO_SEGMENT}",
                    "orderId": None
                }
            exchange_segment = self._default_exchange_segment
            if index_name:
                try:
                    index_config = get_index_config(index_name)
                    if not index_config:
                        raise ValueError(f"Unknown index: {index_name}")
                    segment_key = index_config.get("fno_segment")
                    if segment_key:
                        # Try exact attribute name first, then uppercase for config/SDK inconsistencies.
                        resolved_segment = getattr(self.dhan, segment_key, None)
                        if resolved_segment is None:
                            resolved_segment = getattr(self.dhan, segment_key.upper(), None)
                        if resolved_segment is None:
                            resolved_segment = self._default_exchange_segment
                            logger.warning(
                                f"[ORDER] Unknown segment '{segment_key}' for {index_name}; using {DEFAULT_FNO_SEGMENT} "
                                "(orders will likely fail for BSE indices like SENSEX; verify index config)"
                            )
                        exchange_segment = resolved_segment
                except Exception as e:
                    logger.warning(f"[ORDER] Falling back to {DEFAULT_FNO_SEGMENT} segment for {index_name}: {e}")

            # Dhan SDK call is synchronous; run in a thread to avoid blocking the event loop.
            response = await asyncio.to_thread(
                self.dhan.place_order,
                security_id=security_id,
                exchange_segment=exchange_segment,
                transaction_type=self.dhan.BUY if transaction_type == "BUY" else self.dhan.SELL,
                quantity=qty,
                order_type=self.dhan.MARKET,
                product_type=self.dhan.INTRA,
                price=0
            )
            
            # Validate response
            if not response:
                logger.error(f"[ORDER] Empty response from Dhan API for {transaction_type} order, qty={qty}")
                return {"status": "error", "message": "Empty response from Dhan", "orderId": None}

            logger.debug(f"[ORDER] Raw Dhan {transaction_type} response: {response}")

            # Dhan API v2 wraps the order result in {'status': 'success', 'data': {'orderId': '...'}}
            # but some SDK versions return the orderId at the top level directly.
            if isinstance(response, dict):
                resp_status = response.get('status', '')

                # Unwrap nested data payload if present
                data_payload = response.get('data', {})
                if isinstance(data_payload, dict) and 'data' in data_payload:
                    data_payload = data_payload['data']

                # Prefer nested data first, then top-level keys
                order_id = (
                    (data_payload.get('orderId') if isinstance(data_payload, dict) else None)
                    or response.get('orderId')
                    or response.get('order_id')
                    or response.get('id')
                    or (data_payload.get('order_id') if isinstance(data_payload, dict) else None)
                )

                if order_id:
                    logger.info(
                        f"[ORDER] {transaction_type} order placed | "
                        f"OrderID: {order_id} | Security: {security_id} | Qty: {qty}"
                    )
                    return {
                        "status": "success",
                        "orderId": str(order_id),
                        "price": 0,  # fill price not available at placement; use verify_order_filled
                        "quantity": qty,
                        "data": response,
                    }

                if resp_status == 'success':
                    # Success but no orderId found -- log full response for debugging
                    logger.warning(
                        f"[ORDER] {transaction_type} order status=success but no orderId found | "
                        f"Full response: {response}"
                    )
                    return {
                        "status": "success",
                        "orderId": "UNKNOWN",
                        "price": 0,
                        "quantity": qty,
                        "data": response,
                    }

            logger.error(f"[ORDER] Unexpected response format for {transaction_type}: {response}")
            return {"status": "error", "message": f"Unexpected response: {response}", "orderId": None}
            
        except Exception as e:
            logger.error(f"[ORDER] Error placing {transaction_type} order: {e}", exc_info=True)
            return {"status": "error", "message": str(e), "orderId": None}
    
    async def get_positions(self) -> list:
        """Get current positions"""
        try:
            response = self.dhan.get_positions()
            if response and 'data' in response:
                return response.get('data', [])
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
        return []    
    async def verify_order_filled(self, order_id: str, security_id: str, expected_qty: int, timeout_seconds: int = 30) -> dict:
        """Verify if an order was actually filled by polling the Dhan order list.

        Dhan API v2 order statuses:
          Open/in-flight : TRANSIT, PENDING, OPEN, AFTER_MARKET_ORDER_REQ_RECEIVED
          Filled         : TRADED (full fill), PART_TRADED (partial fill)
          Terminal       : CANCELLED, REJECTED, EXPIRED

        For market orders during live hours TRADED typically arrives within 1-3 seconds.
        PART_TRADED means some qty was filled -- we accept that as a usable fill.

        Returns:
            {
                "filled": bool,
                "order_id": str,
                "status": str,
                "filled_qty": int,
                "average_price": float,
                "message": str
            }
        """
        import asyncio

        # Dhan v2 status sets
        FILLED_STATUSES   = {'TRADED', 'COMPLETE', 'COMPLETED', 'FILLED'}
        PARTIAL_STATUSES  = {'PART_TRADED', 'PARTIALLY_TRADED', 'PARTIAL'}
        OPEN_STATUSES     = {'TRANSIT', 'PENDING', 'OPEN', 'AFTER_MARKET_ORDER_REQ_RECEIVED', 'AMO_REQ_RECEIVED'}
        REJECTED_STATUSES = {'REJECTED'}
        CANCEL_STATUSES   = {'CANCELLED', 'EXPIRED', 'CANCELPENDING'}

        try:
            start_time = datetime.now(timezone.utc)
            retry_count = 0
            last_log_time = start_time

            while True:
                retry_count += 1
                elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()

                if elapsed > timeout_seconds:
                    logger.warning(
                        f"[ORDER] x Order {order_id} not confirmed after {timeout_seconds}s "
                        f"(attempt #{retry_count}) -- treating as not filled"
                    )
                    return {
                        "filled": False,
                        "order_id": order_id,
                        "status": "TIMEOUT",
                        "filled_qty": 0,
                        "average_price": 0,
                        "message": f"Order not confirmed within {timeout_seconds}s",
                    }

                if (datetime.now(timezone.utc) - last_log_time).total_seconds() >= 10:
                    logger.info(
                        f"[ORDER] Waiting for order {order_id} to fill... "
                        f"({elapsed:.0f}s elapsed, attempt #{retry_count})"
                    )
                    last_log_time = datetime.now(timezone.utc)

                try:
                    # --- Primary: get_order_by_id (single order, faster + less bandwidth) ---
                    order = None
                    try:
                        resp = await asyncio.to_thread(self.dhan.get_order_by_id, order_id)
                        if resp and resp.get('status') == 'success':
                            data = resp.get('data', {})
                            # Dhan sometimes nests: {'data': {'data': {...}}}
                            if isinstance(data, dict) and 'data' in data and isinstance(data['data'], dict):
                                data = data['data']
                            if isinstance(data, dict) and data.get('orderId'):
                                order = data
                    except Exception:
                        pass  # Fall through to full order list scan

                    # --- Fallback: scan full order list ---
                    if order is None:
                        orders_resp = await asyncio.to_thread(self.dhan.get_order_list)
                        if orders_resp and 'data' in orders_resp:
                            raw = orders_resp['data']
                            # Unwrap nested data if present
                            if isinstance(raw, dict) and 'data' in raw:
                                raw = raw['data']
                            if isinstance(raw, list):
                                for o in raw:
                                    if str(o.get('orderId')) == str(order_id):
                                        order = o
                                        break

                    if order is None:
                        # Order not visible yet -- TRANSIT state can lag by 1-2s
                        logger.debug(
                            f"[ORDER] {order_id} not in order list yet "
                            f"(attempt #{retry_count}, {elapsed:.1f}s)"
                        )
                        await asyncio.sleep(0.5)
                        continue

                    # --- Parse order fields (Dhan v2 field names) ---
                    raw_status = str(
                        order.get('orderStatus') or order.get('status') or ''
                    ).strip().upper()

                    filled_qty = 0
                    try:
                        filled_qty = int(
                            order.get('filledQty')
                            or order.get('filled_qty')
                            or order.get('tradedQuantity')
                            or 0
                        )
                    except Exception:
                        filled_qty = 0

                    average_price = 0.0
                    try:
                        average_price = float(
                            order.get('averagePrice')
                            or order.get('average_price')
                            or order.get('tradedPrice')
                            or 0
                        )
                    except Exception:
                        average_price = 0.0

                    # Dhan v2 uses 'rejectionReason' (not 'reason')
                    rejection_reason = (
                        order.get('rejectionReason')
                        or order.get('reason')
                        or order.get('rejection_reason')
                        or 'Unknown'
                    )

                    logger.debug(
                        f"[ORDER] Poll #{retry_count} | ID={order_id} | "
                        f"Status={raw_status} | FilledQty={filled_qty} | AvgPrice={average_price}"
                    )

                    # --- Fully filled ---
                    if raw_status in FILLED_STATUSES:
                        logger.info(
                            f"[ORDER] + Order {order_id} TRADED (fully filled) | "
                            f"FilledQty={filled_qty} | AvgPrice={average_price} | attempt #{retry_count}"
                        )
                        return {
                            "filled": True,
                            "order_id": order_id,
                            "status": "TRADED",
                            "filled_qty": filled_qty,
                            "average_price": average_price,
                            "message": f"Order fully filled at avg price {average_price}",
                        }

                    # --- Partially filled -- keep polling, accept when full qty reached ---
                    if raw_status in PARTIAL_STATUSES:
                        if filled_qty >= expected_qty:
                            logger.info(
                                f"[ORDER] + Order {order_id} PART_TRADED (full qty reached via partials) | "
                                f"FilledQty={filled_qty} | AvgPrice={average_price}"
                            )
                            return {
                                "filled": True,
                                "order_id": order_id,
                                "status": "PART_TRADED",
                                "filled_qty": filled_qty,
                                "average_price": average_price,
                                "message": f"Partial fills completed full qty at avg {average_price}",
                            }
                        logger.debug(
                            f"[ORDER] PART_TRADED in progress | "
                            f"FilledQty={filled_qty}/{expected_qty} | {elapsed:.1f}s"
                        )
                        await asyncio.sleep(0.5)
                        continue

                    # --- Still open / in-flight ---
                    if raw_status in OPEN_STATUSES:
                        logger.debug(
                            f"[ORDER] Order {order_id} still open | "
                            f"Status={raw_status} | {elapsed:.1f}s elapsed"
                        )
                        await asyncio.sleep(0.5)
                        continue

                    # --- Terminal: rejected ---
                    if raw_status in REJECTED_STATUSES:
                        logger.error(
                            f"[ORDER] x Order {order_id} REJECTED | "
                            f"Reason: {rejection_reason}"
                        )
                        return {
                            "filled": False,
                            "order_id": order_id,
                            "status": "REJECTED",
                            "filled_qty": 0,
                            "average_price": 0,
                            "message": f"Order rejected: {rejection_reason}",
                        }

                    # --- Terminal: cancelled / expired ---
                    if raw_status in CANCEL_STATUSES:
                        logger.warning(f"[ORDER] x Order {order_id} {raw_status}")
                        return {
                            "filled": False,
                            "order_id": order_id,
                            "status": raw_status,
                            "filled_qty": filled_qty,
                            "average_price": average_price,
                            "message": f"Order {raw_status.lower()}",
                        }

                    # Unknown status -- log and keep polling until timeout
                    logger.warning(
                        f"[ORDER] Unrecognised order status '{raw_status}' for {order_id} -- "
                        f"continuing to poll (attempt #{retry_count}, {elapsed:.1f}s)"
                    )
                    await asyncio.sleep(0.5)

                except Exception as e:
                    elapsed = (datetime.now(timezone.utc) - start_time).total_seconds()
                    logger.debug(
                        f"[ORDER] Error polling order {order_id}: {e} "
                        f"(attempt #{retry_count}, {elapsed:.1f}s)"
                    )
                    await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"[ORDER] Fatal error in verify_order_filled: {e}", exc_info=True)
            return {
                "filled": False,
                "order_id": order_id,
                "status": "ERROR",
                "filled_qty": 0,
                "average_price": 0,
                "message": str(e),
            }
