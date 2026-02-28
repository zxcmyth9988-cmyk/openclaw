FROM node:20-slim AS base
RUN npm install -g pnpm
WORKDIR /app

# 复制根依赖清单
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./

# 复制所有子包的 package.json
COPY apps/*/package.json ./apps/
COPY packages/*/package.json ./packages/

# 安装依赖
RUN pnpm install --frozen-lockfile

# 复制全部源代码
COPY . .

# 构建指定应用（如果没有构建步骤可删除此行）
RUN pnpm build --filter makemoneywithai

EXPOSE 18789
ENV NODE_ENV=production
ENV PORT=18789

# 启动指定应用
CMD ["pnpm", "--filter", "makemoneywithai", "start"]
