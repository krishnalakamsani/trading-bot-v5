FROM node:20-alpine AS builder

WORKDIR /app

# Copy package files
COPY ./frontend/package.json ./frontend/yarn.lock* ./

# Install dependencies
RUN yarn install --ignore-engines

# Copy frontend source
COPY ./frontend /app

# Build the application
ARG REACT_APP_BACKEND_URL
ENV REACT_APP_BACKEND_URL=$REACT_APP_BACKEND_URL
ARG REACT_APP_WS_TOKEN
ENV REACT_APP_WS_TOKEN=$REACT_APP_WS_TOKEN
RUN yarn build

# Production stage with nginx
FROM nginx:alpine

# Copy custom nginx config
COPY ./frontend/nginx.conf /etc/nginx/conf.d/default.conf

# Copy built files
COPY --from=builder /app/build /usr/share/nginx/html

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
