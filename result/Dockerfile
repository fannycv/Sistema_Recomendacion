FROM node:18-slim

WORKDIR /app

COPY package*.json ./
COPY . .

RUN npm install

EXPOSE 3000

CMD ["node", "server.js"]