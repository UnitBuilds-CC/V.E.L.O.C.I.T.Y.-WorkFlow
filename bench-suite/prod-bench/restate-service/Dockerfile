FROM node:22-slim

WORKDIR /app

COPY package.json .
RUN npm install

COPY service.js .

EXPOSE 9080

CMD ["node", "service.js"]
