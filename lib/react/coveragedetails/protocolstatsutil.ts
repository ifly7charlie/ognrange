import {layerFromDestCallsign, LAYER_COLOR, Layer} from '../../common/layers';
import graphcolours from '../graphcolours';

// All individual layer keys to sum for the "all" tab (excludes "combined" to avoid double-counting)
export const INDIVIDUAL_LAYERS = ['flarm', 'adsb', 'adsl', 'fanet', 'ogntrk', 'paw', 'safesky'];

export function rgbToHex([r, g, b]: [number, number, number]): string {
    return '#' + [r, g, b].map((c) => c.toString(16).padStart(2, '0')).join('');
}

export function layerColorForTocall(tocall: string): string {
    const layer = layerFromDestCallsign(tocall);
    return layer ? rgbToHex(LAYER_COLOR[layer]) : '#999';
}

export function colorForTab(selectedTab: string): string {
    if (selectedTab === 'all') return graphcolours[0];
    const layer = selectedTab as Layer;
    if (LAYER_COLOR[layer]) return rgbToHex(LAYER_COLOR[layer]);
    return graphcolours[0];
}
