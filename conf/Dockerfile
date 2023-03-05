FROM node:16-alpine as gyp
RUN apk add --no-cache --virtual .build-deps alpine-sdk python3
WORKDIR /ognrange
COPY .git/HEAD .git/HEAD
COPY .git/refs/heads .git/refs/heads
COPY ./conf/.env.local ./.env.local
RUN echo "NEXT_PUBLIC_GIT_REF=$(cat /ognrange/.git/`cat .git/HEAD | cut -f2 -d ' '`)" >> ./.env.local
RUN rm -rf .git

FROM gyp as build-image
WORKDIR /ognrange
ARG NEXT_PUBLIC_SITEURL
ARG NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN
COPY ./package.json ./
COPY ./yarn.lock ./
RUN npm_config_build_from_source=true yarn install

FROM build-image AS next-build-image
COPY ./next.config.cjs ./next.config.cjs
COPY ./lib/common ./lib/common
COPY ./lib/api ./lib/api
COPY ./pages ./pages
COPY ./lib/react ./lib/react
COPY ./styles ./styles
RUN yarn next build

FROM build-image AS aprs-build-image
COPY ./conf/.env.local ./.env.local
COPY ./lib/common ./lib/common
COPY ./bin ./bin
COPY ./lib/bin ./lib/bin
COPY ./tsconfig-bin.json ./tsconfig-bin.json
RUN yarn aprs:build

FROM gyp as base-image
WORKDIR /ognrange
COPY ./package.json ./
COPY ./yarn.lock ./
RUN npm_config_build_from_source=true yarn install --production  --frozen-lockfile
RUN apk del .build-deps

FROM base-image AS next
WORKDIR /ognrange
COPY --from=next-build-image /ognrange/.next ./.next
COPY ./public ./public
COPY ./next.config.cjs ./next.config.cjs
COPY ./LICENSE ./LICENSE
EXPOSE 3000
CMD yarn next start

FROM base-image AS aprs
WORKDIR /ognrange
RUN mkdir data
COPY --from=aprs-build-image /ognrange/dist ./dist
RUN cat /ognrange/.env.local
CMD node dist/bin/aprs.js

FROM httpd:2.4-alpine AS apache
RUN mkdir -p /ognrange/data
COPY ./conf/docker-httpd.conf /usr/local/apache2/conf/httpd.conf
