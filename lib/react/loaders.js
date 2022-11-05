//
//
// Helper functions for loading data from APIs
//
// These will be used throughout the components, but it's tidiest to keep the functions in one place
//

import {Icon} from '../lib/htmlhelper.js';

//
// Loading helpers
export function Spinner() {
    return (
        <div>
            <Icon type="plane" spin={true} />
        </div>
    );
}

export function Error() {
    return <div>Oops!</div>;
}
