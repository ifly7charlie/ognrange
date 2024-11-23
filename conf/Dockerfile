# disposable to find github version, only used for that
FROM node:20-alpine AS version
WORKDIR /ognrange
COPY .git/HEAD .git/HEAD
COPY .git/refs/heads .git/refs/heads
COPY ./conf/.env.local ./.env.local
RUN echo "NEXT_PUBLIC_GIT_REF=$(cat /ognrange/.git/`cat .git/HEAD | cut -f2 -d ' '`)" >> ./.env.local

######## builders
FROM node:20-alpine AS gyp

FROM gyp AS build-image
WORKDIR /ognrange
ARG NEXT_PUBLIC_SITEURL
ARG NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN
COPY ./package.json ./
COPY ./yarn.lock ./
RUN apk add --no-cache --virtual .build-deps alpine-sdk python3 && npm_config_build_from_source=true yarn install --frozen-lockfile && apk del .build-deps

FROM build-image AS next-build-image
COPY ./next.config.js ./next.config.js
COPY ./tsconfig.json ./tsconfig.json
COPY ./styles ./styles
COPY ./lib/common ./lib/common
COPY ./lib/api ./lib/api
COPY ./pages ./pages
COPY ./lib/react ./lib/react
COPY --from=version /ognrange/.env.local ./.env.local
RUN yarn next build

FROM build-image AS aprs-build-image
COPY ./lib/common ./lib/common
COPY ./bin ./bin
COPY ./lib/bin ./lib/bin
COPY ./lib/worker ./lib/worker
COPY ./tsconfig-bin.json ./tsconfig-bin.json
COPY --from=version /ognrange/.env.local ./.env.local
RUN yarn aprs:build


########### production images
FROM build-image AS base-image
WORKDIR /ognrange
RUN yarn install --production  --frozen-lockfile


FROM node:20-alpine AS node-base
WORKDIR /ognrange
COPY --from=build-image /ognrange/node_modules ./node_modules
COPY ./LICENSE ./LICENSE

FROM node-base AS next
HEALTHCHECK --interval=10s --retries=5 CMD wget --no-verbose --tries=1 --spider http://next:3000/ || exit 1
WORKDIR /ognrange
COPY ./next.config.js ./next.config.js
COPY ./package.json ./
COPY ./public ./public
COPY --from=next-build-image /ognrange/.next/static ./.next/static
COPY --from=next-build-image /ognrange/.next/ ./.next/
COPY --from=next-build-image /ognrange/.env.local ./.env.local
EXPOSE 3000
CMD yarn next start

FROM node-base AS aprs
HEALTHCHECK --interval=10s --retries=5 CMD flock --exclusive --nonblock /ognrange/data/db/status/LOCK  --command 'echo lock failed' || exit 1
WORKDIR /ognrange
RUN mkdir data
COPY --from=aprs-build-image /ognrange/dist ./dist
COPY --from=aprs-build-image /ognrange/package.json .
COPY --from=aprs-build-image /ognrange/.env.local ./.env.local
CMD node dist/bin/aprs.js

FROM httpd:2.4-alpine AS apache
HEALTHCHECK --interval=10s --retries=5 CMD wget --no-verbose --tries=1 --spider http://apache:8000/ || exit 1
RUN mkdir -p /ognrange/data
EXPOSE 8443
EXPOSE 8000
COPY ./conf/docker-httpd.conf /usr/local/apache2/conf/httpd.conf
COPY ./conf/nope.html /ognrange/data/