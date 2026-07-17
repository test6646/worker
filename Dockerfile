# Use Node.js 22 (native WebSocket support)
FROM node:22-alpine

WORKDIR /app

# Install dependencies
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund

# Copy application
COPY src ./src

# Expose Railway port
EXPOSE 3000

# Start the server
CMD ["npm", "start"]
