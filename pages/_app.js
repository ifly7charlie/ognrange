//import '../public/bootstrap/css/font-awesome.min.css';
import 'mapbox-gl/dist/mapbox-gl.css';
import '../styles/styles.css';

//import '../styles/onglide.scss';

import { useState } from 'react';

// This default export is required in a new `pages/_app.js` file.
export default function MyApp({ Component, pageProps }) {
    const [ options, setOptions ] = useState( { mapType: 0 } );
    return <Component {...pageProps} options={options} setOptions={setOptions} />
}
			
