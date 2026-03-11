import {useTranslation} from 'next-i18next';
import {LAYER_COLOR, LAYER_BIT, Layer, layersFromBitfield} from '../common/layers';

/** Colored square badges for each layer in a bitmask. Used in station details and visualisation legend. */
export function LayerBadges({layerMask}: {layerMask?: number}) {
    const {t} = useTranslation();
    const mask = layerMask || 1 << LAYER_BIT[Layer.COMBINED];
    const layers = layersFromBitfield(mask);

    return (
        <div style={{display: 'flex', flexWrap: 'wrap', gap: '6px', marginTop: '4px', marginBottom: '4px', fontSize: '0.85em'}}>
            {layers.map((l) => {
                const color = LAYER_COLOR[l];
                return (
                    <div key={l} style={{display: 'flex', alignItems: 'center', gap: '4px'}}>
                        <div
                            style={{
                                width: '14px',
                                height: '14px',
                                borderRadius: '2px',
                                flexShrink: 0,
                                backgroundColor: `rgb(${color[0]},${color[1]},${color[2]})`
                            }}
                        />
                        <span>{t(`layers.${l}`)}</span>
                    </div>
                );
            })}
        </div>
    );
}
