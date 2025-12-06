//import '../public/bootstrap/css/font-awesome.min.css';
import 'mapbox-gl/dist/mapbox-gl.css';
import '../styles/styles.css';

import {appWithTranslation} from 'next-i18next';

import {StationMeta} from '../lib/react/stationmeta';
import {DisplayedH3s} from '../lib/react/displayedh3s';

//import '../styles/onglide.scss';

//import {useState} from 'react';

// This default export is required in a new `pages/_app.js` file.
function MyApp({Component, pageProps}) {
    return (
        <StationMeta env={pageProps.env}>
            <DisplayedH3s env={pageProps.env}>
                <Component {...pageProps} />
            </DisplayedH3s>
        </StationMeta>
    );
}

export default appWithTranslation(MyApp);
