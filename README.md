
## Getting Started


```bash
npm run dev
# or
yarn dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

You can start editing the page by modifying `pages/index.js`. The page auto-updates as you edit the file.

[API routes](https://nextjs.org/docs/api-routes/introduction) can be accessed on [http://localhost:3000/api/hello](http://localhost:3000/api/hello). This endpoint can be edited in `pages/api/hello.js`.

The `pages/api` directory is mapped to `/api/*`. Files in this directory are treated as [API routes](https://nextjs.org/docs/api-routes/introduction) instead of React pages.

## Learn More




## rebuilding protobuf

cd lib; pbjs --target json --wrap es6 range.proto -o range-protobuf.mjs; cd -
cd lib; pbjs --target json range.proto -o range-protobuf.js; cd -

then you need to frig it so they export - recommend to check what is missing from top and bottom of file after rebuilding
