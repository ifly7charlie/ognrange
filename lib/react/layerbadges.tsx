import {useCallback} from 'react';
import {useTranslation} from 'next-i18next';
import {LAYER_COLOR, LAYER_BIT, Layer, layersFromBitfield} from '../common/layers';

/** Colored square badges for each layer in a bitmask. Used in station details and visualisation legend.
 *  When layers/setLayers are provided, badges become clickable toggles. */
export function LayerBadges({layerMask, layers, setLayers}: {layerMask?: number; layers?: string[]; setLayers?: (l: string[]) => void}) {
    const {t} = useTranslation();
    const mask = layerMask || 1 << LAYER_BIT[Layer.COMBINED];
    const available = layersFromBitfield(mask);
    const clickable = !!setLayers;

    const toggle = useCallback(
        (layer: string) => {
            if (!setLayers || !layers) return;
            const isSelected = layers.includes(layer);
            if (isSelected) {
                const remaining = layers.filter((l) => l !== layer);
                setLayers(remaining.length ? remaining : ['combined']);
            } else {
                setLayers([...layers, layer]);
            }
        },
        [layers, setLayers]
    );

    return (
        <div style={{display: 'flex', flexWrap: 'wrap', gap: '6px', marginTop: '4px', marginBottom: '4px', fontSize: '0.85em'}}>
            {available.map((l) => {
                const color = LAYER_COLOR[l];
                const rgb = `rgb(${color[0]},${color[1]},${color[2]})`;
                const active = !layers || layers.includes(l);
                return (
                    <div
                        key={l}
                        onClick={clickable ? () => toggle(l) : undefined}
                        style={{
                            display: 'flex',
                            alignItems: 'center',
                            gap: '4px',
                            cursor: clickable ? 'pointer' : undefined,
                            opacity: active ? 1 : 0.35
                        }}
                    >
                        <div
                            style={{
                                width: '14px',
                                height: '14px',
                                borderRadius: '2px',
                                flexShrink: 0,
                                backgroundColor: rgb
                            }}
                        />
                        <span>{t(`layers.${l}`)}</span>
                    </div>
                );
            })}
        </div>
    );
}
