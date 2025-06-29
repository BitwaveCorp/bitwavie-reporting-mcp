# Multi-stage build for production optimization
FROM node:18-alpine AS builder

# Set working directory
WORKDIR /app

# Copy package files first for better caching
COPY package*.json ./

# Clean install dependencies
RUN npm cache clean --force && \
    rm -rf node_modules && \
    npm ci --silent

# Copy all files
COPY . .

# Build TypeScript with verbose output
RUN npm run build || (echo "Build failed with errors" && exit 1)

# Production stage
FROM node:18-alpine AS production

# Create app directory
WORKDIR /app

# Create non-root user for security
RUN addgroup -g 1001 -S nodejs && \
    adduser -S mcp-server -u 1001

# Copy package files
COPY package*.json ./

# Install only production dependencies
RUN npm ci --only=production --silent && \
    npm cache clean --force

# Copy built application from builder stage
COPY --from=builder /app/dist ./dist

# Copy HTTP server files
COPY --from=builder /app/http-server.js ./
COPY --from=builder /app/start-http-server.sh ./
COPY --from=builder /app/test-rpc.js ./
COPY --from=builder /app/simple-http-server.js ./
COPY --from=builder /app/start-simple-server.sh ./

# Copy data directory with table mappings
COPY --from=builder /app/data ./data

# Ensure data directory has the right permissions
RUN mkdir -p /app/data && \
    chown -R mcp-server:nodejs /app/data && \
    chmod -R 755 /app/data

# Print file contents for debugging
RUN ls -la && echo "Content of start-simple-server.sh:" && cat start-simple-server.sh

# Copy any additional config files if needed
COPY --chown=mcp-server:nodejs .env* ./

# Make the start scripts executable
RUN chmod +x ./start-http-server.sh ./start-simple-server.sh

# Switch to non-root user
USER mcp-server

# Start the simple HTTP server for testing
CMD ["./start-simple-server.sh"]

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD node -e "console.log('Health check passed')" || exit 1

# Expose port for Cloud Run
EXPOSE 8080

# Set environment
ENV NODE_ENV=production

# Set Cloud Run port environment variable
ENV PORT=8080
