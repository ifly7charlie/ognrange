# Images carry no site config: NEXT_PUBLIC_* and aprs settings are read from
# the container environment at runtime (see pages/index.tsx getServerSideProps).
# Only GIT_REF is fixed at build time.
#
# Base images are pinned by digest; dependabot keeps them current.

######## builders
# glibc (debian) so node_modules match the distroless runtime
FROM node:26-trixie@sha256:a98e6fda54fdbb22d4238054cca9a2a113bcd1191c09193a535aae22f400aa8e AS build-image
WORKDIR /ognrange
COPY ./package.json ./
COPY ./yarn.lock ./
RUN npm_config_build_from_source=true yarn install --frozen-lockfile

FROM build-image AS next-build-image
ARG GIT_REF=unknown
ENV NEXT_PUBLIC_GIT_REF=$GIT_REF
COPY ./next.config.js ./next.config.js
COPY ./next-i18next.config.js ./next-i18next.config.js
COPY ./tsconfig.json ./tsconfig.json
COPY ./styles ./styles
COPY ./lib/common ./lib/common
COPY ./lib/api ./lib/api
COPY ./pages ./pages
COPY ./lib/react ./lib/react
# build cache isn't needed at runtime
RUN yarn next build && rm -rf .next/cache

FROM rust:1-alpine@sha256:b1d5dd5a22a09404d3e798c0cf051b689077360374eb505fa5bb93c479693d87 AS aprs-build-image
RUN apk add --no-cache musl-dev
WORKDIR /aprs-server
COPY ./aprs-server/Cargo.toml ./aprs-server/Cargo.lock ./
# pre-build dependencies for layer caching
RUN mkdir src && echo 'fn main() {}' > src/main.rs && cargo build --release && rm -rf src
COPY ./aprs-server/src ./src
RUN touch src/main.rs && cargo build --release

# production dependencies only
FROM build-image AS base-image
WORKDIR /ognrange
RUN yarn install --production --frozen-lockfile


########### production images
# distroless: no shell, so CMD/HEALTHCHECK must be exec form
FROM gcr.io/distroless/nodejs26-debian13:nonroot@sha256:3a6f4e6fd37efe00042667d06fd52240f2589ed09b0694fbb3b1bf860030a839 AS next
ARG GIT_REF=unknown
ENV NEXT_PUBLIC_GIT_REF=$GIT_REF
ENV NODE_ENV=production
HEALTHCHECK --interval=10s --retries=5 CMD ["/nodejs/bin/node", "-e", "fetch('http://127.0.0.1:3000/').then(r=>process.exit(r.ok?0:1),()=>process.exit(1))"]
WORKDIR /ognrange
COPY --from=base-image /ognrange/node_modules ./node_modules
COPY ./LICENSE ./LICENSE
COPY ./next.config.js ./next.config.js
COPY ./next-i18next.config.js ./next-i18next.config.js
COPY ./package.json ./
COPY ./public ./public
COPY --from=next-build-image /ognrange/.next/ ./.next/
EXPOSE 3000
# entrypoint is node
CMD ["node_modules/next/dist/bin/next", "start"]

# static musl binary, rustls with webpki-roots: no libc or CA bundle needed.
# Runs as root so existing db volume ownership keeps working.
FROM gcr.io/distroless/static-debian13@sha256:034df5c1737e8241f6ed6423a12e720ee0f6d6f088609ee172f98f6f2166ef5d AS aprs
ARG GIT_REF=unknown
ENV GIT_REF=$GIT_REF
WORKDIR /ognrange
COPY --from=aprs-build-image /aprs-server/target/release/aprs-server ./aprs-server
CMD ["/ognrange/aprs-server"]

FROM httpd:2.4-alpine@sha256:759a1d9178d351185978e41be37a3e91675a4ddc0653aa5df493cfd4c67e24ec AS apache
HEALTHCHECK --interval=10s --retries=5 CMD wget --no-verbose --tries=1 --spider http://apache:8000/ || exit 1
RUN mkdir -p /ognrange/data
EXPOSE 8443
EXPOSE 8000
COPY ./conf/docker-httpd.conf /usr/local/apache2/conf/httpd.conf
COPY ./conf/nope.html /ognrange/data/
