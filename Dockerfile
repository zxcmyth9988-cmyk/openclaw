FROM node:20-slim
RUN npm install -g pnpm
WORKDIR /app
COPY . .
RUN pnpm install --frozen-lockfile
RUN pnpm build || true
EXPOSE 18789
ENV NODE_ENV=production
ENV PORT=18789
CMD ["pnpm", "start"]
