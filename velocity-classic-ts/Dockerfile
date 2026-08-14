# syntax=docker/dockerfile:1
FROM node:20-slim

WORKDIR /app
COPY velocity-classic-ts/package.json velocity-classic-ts/tsconfig.json ./
RUN npm install

COPY velocity-classic-ts/src ./src
RUN npx tsc

EXPOSE 8083
CMD ["node", "dist/main.js"]
