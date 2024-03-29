//
//
// This class overrides the PathLayer point selection algorithm which normally just
// tells you what line segment is covered. By overriding this we can identify exactly
// what point on the line is hovered/clicked and use that to display information about
// the specific spot on the trace like the time or climb rate.
//
//
// FWIW If a plane has good flarm coverage there will only be one segment as we
// only generate a new segment on gaps. Mapbox recommended one segment for each colour
// but that isn't needed for deckgl binary layers as we can specify a colour per vertex
// if we want. It also means that each segment is rendered as a line and there is no
// joining or smoothing which is less than ideal
//

import {H3HexagonLayer} from '@deck.gl/geo-layers';
import GL from '@luma.gl/constants';

export class PickableHexagonLayer extends H3HexagonLayer {
    initializeState() {
        super.initializeState();

        this.getAttributeManager().addInstanced({
            instancePickingColors: {
                size: 3,
                type: GL.UNSIGNED_BYTE,
                update: this.calculatePickingColors
            }
        });
    }

    // Deckgl generates an offscreen pixmap that it renders z-order into and the
    // colour is then used to figure out what has been picked. We use the index
    // from the start of the timing array to determine the picking colour
    calculatePickingColors(attribute) {
        const {data} = this.props;
        const {value} = attribute;

        let i = 0;
        for (const object of data.h) {
            const pickingColor = this.encodePickingColor(i);
            value[i * 3] = pickingColor[0];
            value[i * 3 + 1] = pickingColor[1];
            value[i * 3 + 2] = pickingColor[2];
            i++;
        }
    }

    // This function is called to convert from colour back into specific data
    // we enrich it with what we can collect from our props.data attributes
    getPickingInfo(pickParams) {
        const info = super.getPickingInfo(pickParams);
        const props = pickParams?.info?.layer?.props;
        if (info.picked && props && props.data) {
            info.object = {...info.object, index: pickParams.info.index};
        }
        return info;
    }
}
PickableHexagonLayer.layerName = 'PickableHexagonLayer';

//module.exports = PickableHexagonLayer;
