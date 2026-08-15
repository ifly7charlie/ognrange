import {QRCodeSVG} from 'qrcode.react';

import {useTranslation} from 'next-i18next';

// Subscribe QR code for the station's outage-notification topic. The backend
// stores the topic as an https://ntfy.sh/... URL; the QR encodes the same
// topic with the ntfy:// scheme so scanning it opens the ntfy app's
// subscribe flow directly.
export function StationNotify({ntfyUrl}: {ntfyUrl?: string | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details'});

    if (!ntfyUrl) {
        return null;
    }

    const subscribeUrl = ntfyUrl.replace(/^https?:\/\//, 'ntfy://');

    return (
        <>
            <br />
            <b>{t('notify.title')}</b>
            <br />
            <div style={{fontSize: 'small', width: '350px', marginBottom: '0.5em'}}>{t('notify.scan')}</div>
            <QRCodeSVG value={subscribeUrl} size={160} marginSize={2} />
            <div style={{fontSize: 'x-small', overflowWrap: 'anywhere', width: '350px'}}>
                <a href={ntfyUrl} target="_blank" rel="noreferrer">
                    {ntfyUrl}
                </a>
            </div>
        </>
    );
}
