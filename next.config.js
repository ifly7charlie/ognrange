const {i18n} = require('./next-i18next.config');
const {execSync} = require('child_process');

function getGitRef() {
    if (process.env.NEXT_PUBLIC_GIT_REF) return process.env.NEXT_PUBLIC_GIT_REF;
    try {
        return execSync('git rev-parse --short HEAD', {encoding: 'utf8'}).trim();
    } catch {
        return 'unknown';
    }
}

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

    env: {
        NEXT_PUBLIC_GIT_REF: getGitRef()
    },

    i18n,

    eslint: {
        // Warning: This allows production builds to successfully complete even if
        // your project has ESLint errors.
        ignoreDuringBuilds: true
    }
};
