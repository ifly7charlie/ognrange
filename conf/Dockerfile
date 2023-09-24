# disposable to find github version, only used for that
FROM node:16-alpine as version
WORKDIR /ognrange
COPY .git/HEAD .git/HEAD
COPY .git/refs/heads .git/refs/heads
COPY ./conf/.env.local ./.env.local
RUN echo "NEXT_PUBLIC_GIT_REF=$(cat /ognrange/.git/`cat .git/HEAD | cut -f2 -d ' '`)" >> ./.env.local

######## builders
FROM node:16-alpine as gyp
RUN apk add --no-cache flock curl
WORKDIR /ognrange
COPY --from=version /ognrange/.env.local ./.env.local

FROM gyp as build-image
WORKDIR /ognrange
ARG NEXT_PUBLIC_SITEURL
ARG NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN
COPY ./package.json ./
COPY ./yarn.lock ./
RUN apk add --no-cache --virtual .build-deps alpine-sdk python3 && npm_config_build_from_source=true yarn install --frozen-lockfile && apk del .build-deps

FROM build-image AS next-build-image
COPY ./next.config.cjs ./next.config.cjs
COPY ./tsconfig.json ./tsconfig.json
COPY ./styles ./styles
COPY ./lib/common ./lib/common
COPY ./lib/api ./lib/api
COPY ./pages ./pages
COPY ./lib/react ./lib/react
RUN yarn next build

FROM build-image AS aprs-build-image
COPY ./lib/common ./lib/common
COPY ./bin ./bin
COPY ./lib/bin ./lib/bin
COPY ./lib/worker ./lib/worker
COPY ./tsconfig-bin.json ./tsconfig-bin.json
RUN yarn aprs:build


########### production images
FROM gyp as base-image
WORKDIR /ognrange
COPY ./package.json ./
COPY ./yarn.lock ./
RUN apk add --no-cache --virtual .build-deps alpine-sdk python3 && npm_config_build_from_source=true yarn install --production  --frozen-lockfile && apk del .build-deps

FROM base-image AS next
HEALTHCHECK --interval=10s --retries=5 CMD curl -Q --fail http://localhost:3000 || exit 1
WORKDIR /ognrange
COPY --from=next-build-image /ognrange/.next ./.next
COPY ./public ./public
COPY ./next.config.cjs ./next.config.cjs
COPY ./LICENSE ./LICENSE
EXPOSE 3000
CMD yarn next start

FROM base-image AS aprs
HEALTHCHECK --interval=10s --retries=5 CMD flock --exclusive --nonblock /ognrange/data/db/status/LOCK  --command 'echo lock failed' || exit 1
WORKDIR /ognrange
RUN mkdir data
COPY --from=aprs-build-image /ognrange/dist ./dist
CMD node dist/bin/aprs.js

FROM httpd:2.4-alpine AS apache
HEALTHCHECK --interval=10s --retries=5 CMD curl -Q --fail http://localhost:8000 || exit 1
RUN apk add --no-cache curl
RUN mkdir -p /ognrange/data
COPY ./conf/docker-httpd.conf /usr/local/apache2/conf/httpd.conf
