import {QRCodeSVG} from 'qrcode.react';

import {useTranslation} from 'next-i18next/pages';

import {formatEpoch} from '../formatdate';

// Outage-notification state from the station JSON (aprs-server ntfy.rs).
// Every field is optional: healthy stations and files written by older
// servers simply don't carry them.
export interface StationOutage {
    outageNotifiedAt?: number | null;
    outageReason?: string | null;
    outageLastHeard?: number | null;
    outageEndedAt?: number | null;
}

// Subscribe QR code for the station's outage-notification topic. The QR
// encodes the https:// topic URL as-is - camera apps don't recognise the
// ntfy:// scheme, and the ntfy web page it opens offers the subscribe flow
// (including a handoff into the app).
export function StationNotify({ntfyUrl, outage}: {ntfyUrl?: string | null; outage?: StationOutage | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details'});

    if (!ntfyUrl) {
        return null;
    }

    return (
        <>
            <br />
            <b>{t('notify.title')}</b>
            <br />
            <OutageState outage={outage} />
            <div style={{fontSize: 'small', width: '350px', marginBottom: '0.5em'}}>{t('notify.scan')}</div>
            <QRCodeSVG value={ntfyUrl} size={160} marginSize={2} />
            <div style={{fontSize: 'x-small', overflowWrap: 'anywhere', width: '350px'}}>
                <a href={ntfyUrl} target="_blank" rel="noreferrer">
                    {ntfyUrl}
                </a>
            </div>
        </>
    );
}

// The outage currently flagged by the notifier, if any. outageNotifiedAt is
// the flag; outageEndedAt means it has recovered but the back-online message
// hasn't gone out yet (held for the station's notify window).
function OutageState({outage}: {outage?: StationOutage | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details'});

    if (!outage?.outageNotifiedAt) {
        return null;
    }

    // Outages flagged before outageLastHeard existed only know when they were notified
    const from = outage.outageLastHeard ? formatEpoch(outage.outageLastHeard) : t('notify.before', {when: formatEpoch(outage.outageNotifiedAt)});
    const reason = outage.outageReason === 'beacons' || outage.outageReason === 'traffic' ? outage.outageReason : 'unknown';
    const text = outage.outageEndedAt ? t(`notify.recovered_${reason}`, {from, until: formatEpoch(outage.outageEndedAt)}) : t(`notify.offline_${reason}`, {from});

    return <div style={{fontSize: 'small', width: '350px', marginBottom: '0.5em', fontWeight: outage.outageEndedAt ? undefined : 'bold'}}>{text}</div>;
}
