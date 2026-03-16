import {useTranslation} from 'next-i18next';
import {formatEpoch} from '../formatdate';

// Small static Mapbox map showing a pin at a location (~30km across at zoom 10)
function LocationMap({lat, lng, label, lastSeen, token}: {lat: number; lng: number; label: string; lastSeen?: number | null; token: string}) {
    const url = `https://api.mapbox.com/styles/v1/mapbox/outdoors-v11/static/pin-s+e74c3c(${lng},${lat})/${lng},${lat},10/300x200@2x?access_token=${token}`;

    return (
        <div style={{flex: 1, minWidth: 0}}>
            <div style={{fontSize: '0.8rem', fontWeight: 'bold', marginBottom: '2px'}}>{label}</div>
            <img src={url} alt={label} style={{width: '100%', height: 'auto', borderRadius: '4px', display: 'block'}} />
            {lastSeen ? (
                <div style={{fontSize: '0.75rem', color: '#666', marginTop: '2px'}}>{formatEpoch(lastSeen)}</div>
            ) : null}
        </div>
    );
}

export function StationPosition({
    mobile,
    moved,
    bouncing,
    primaryLocation,
    previousLocation,
    lastSeenAtPrimary,
    lastSeenAtPrevious,
    mapboxToken
}: {
    mobile?: boolean;
    moved?: boolean;
    bouncing?: boolean;
    primaryLocation?: [number, number] | null;
    previousLocation?: [number, number] | null;
    lastSeenAtPrimary?: number | null;
    lastSeenAtPrevious?: number | null;
    mapboxToken: string;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.position'});

    if (mobile) {
        return (
            <>
                <hr />
                <b>{t('title')}</b>
                <br />
                {t('mobile')}
                <hr />
            </>
        );
    }

    if (moved) {
        return (
            <>
                <hr />
                <b>{t('title')}</b>
                <br />
                {t('moved')}
                <hr />
            </>
        );
    }

    if (!bouncing) return null;

    const hasBothLocations = primaryLocation && previousLocation;

    return (
        <>
            <hr />
            <b>{'\u26A0\uFE0F '}{t('title')}</b>
            <br />
            {t('bouncing')}
            <br />
            {hasBothLocations && mapboxToken ? (
                <div style={{marginTop: '4px', display: 'flex', gap: '8px'}}>
                    <LocationMap lat={primaryLocation[0]} lng={primaryLocation[1]} label={t('primary')} lastSeen={lastSeenAtPrimary} token={mapboxToken} />
                    <LocationMap lat={previousLocation[0]} lng={previousLocation[1]} label={t('previous')} lastSeen={lastSeenAtPrevious} token={mapboxToken} />
                </div>
            ) : null}
            <hr />
        </>
    );
}
