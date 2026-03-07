const {i18n} = require('./next-i18next.config');

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

    i18n,

    eslint: {
        // Warning: This allows production builds to successfully complete even if
        // your project has ESLint errors.
        ignoreDuringBuilds: true
    }
};
