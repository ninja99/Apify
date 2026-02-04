FROM apify/actor-node:22

WORKDIR /usr/src/app

COPY package*.json ./
RUN npm install --omit=dev

COPY . ./

CMD ["npm", "start"]