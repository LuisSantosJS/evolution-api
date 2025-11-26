FROM node:22 AS builder

RUN apt-get update && \
    apt-get install -y git ffmpeg wget curl bash openssl dos2unix build-essential libvips-dev && \
    rm -rf /var/lib/apt/lists/*

LABEL version="2.3.1" description="Api to control whatsapp features through http requests."
LABEL maintainer="Davidson Gomes" git="https://github.com/DavidsonGomes"
LABEL contact="contato@evolution-api.com"

WORKDIR /evolution

COPY ./package*.json ./
COPY ./tsconfig.json ./
COPY ./tsup.config.ts ./

# Install dependencies with proper flags
RUN npm install --legacy-peer-deps --ignore-engines
# Install Rollup for linux x64 gnu
RUN npm install @rollup/rollup-linux-x64-gnu --legacy-peer-deps --ignore-engines
# Install sharp with specific platform flags
RUN npm install sharp --platform=linux --arch=x64 --legacy-peer-deps --ignore-engines

COPY ./src ./src
COPY ./public ./public
COPY ./prisma ./prisma
COPY ./manager ./manager
COPY ./.env.example ./.env
COPY ./runWithProvider.js ./

COPY ./Docker ./Docker

RUN chmod +x ./Docker/scripts/* && dos2unix ./Docker/scripts/*

RUN ./Docker/scripts/generate_database.sh

RUN npm run build

FROM node:22 AS final

RUN apt-get update && \
    apt-get install -y tzdata ffmpeg bash openssl && \
    rm -rf /var/lib/apt/lists/*

# Garantir que o diretório /tmp existe com permissões corretas
RUN mkdir -p /tmp && chmod 1777 /tmp

ENV TZ=America/Sao_Paulo
ENV DOCKER_ENV=true

WORKDIR /evolution

COPY --from=builder /evolution/package.json ./package.json
COPY --from=builder /evolution/package-lock.json ./package-lock.json

COPY --from=builder /evolution/node_modules ./node_modules
COPY --from=builder /evolution/dist ./dist
COPY --from=builder /evolution/prisma ./prisma
COPY --from=builder /evolution/manager ./manager
COPY --from=builder /evolution/public ./public
COPY --from=builder /evolution/.env ./.env
COPY --from=builder /evolution/Docker ./Docker
COPY --from=builder /evolution/runWithProvider.js ./runWithProvider.js
COPY --from=builder /evolution/tsup.config.ts ./tsup.config.ts

ENV DOCKER_ENV=true

EXPOSE 8080

ENTRYPOINT ["/bin/bash", "-c", ". ./Docker/scripts/deploy_database.sh && npm run start:prod" ]
