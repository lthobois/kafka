FROM node:24.14.0-bookworm-slim
WORKDIR /workspace
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
ENTRYPOINT ["node", "src/cli.ts"]
