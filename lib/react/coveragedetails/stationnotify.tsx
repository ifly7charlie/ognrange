import {QRCodeSVG} from 'qrcode.react';

import {useTranslation} from 'next-i18next';

// Subscribe QR code for the station's outage-notification topic. The QR
// encodes the https:// topic URL as-is - camera apps don't recognise the
// ntfy:// scheme, and the ntfy web page it opens offers the subscribe flow
// (including a handoff into the app).
export function StationNotify({ntfyUrl}: {ntfyUrl?: string | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details'});

    if (!ntfyUrl) {
        return null;
    }

    return (
        <>
            <br />
            <b>{t('notify.title')}</b>
            <br />
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
