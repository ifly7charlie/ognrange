module.exports = {
    //	webpack5: true,
    productionBrowserSourceMaps: true,
    reactStrictMode: true,
    webpack: (config, {buildId, dev, isServer, defaultLoaders, webpack}) => {
        config.optimization.minimize = true;
        //		config.resolve.extensions =
        //			[ '.ts', '.mjs', '.js', '.jsx', '.json', '.wasm' ];
        // Important: return the modified config
        //		console.log( config );
        return config;
    },
    eslint: {
        // Warning: This allows production builds to successfully complete even if
        // your project has ESLint errors.
        ignoreDuringBuilds: true
    },
    serverRuntimeConfig: {
        NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN: process.env.NEXT_PUBLIC_MAPBOX_ACCESS_TOKEN,
        NEXT_PUBLIC_SITEURL: process.env.NEXT_PUBLIC_SITEURL,
        NEXT_PUBLIC_DATA_URL: process.env.NEXT_PUBLIC_DATA_URL,
        NEXT_PUBLIC_AIRSPACE_API_KEY: process.env.NEXT_PUBLIC_AIRSPACE_API_KEY
    }
};
