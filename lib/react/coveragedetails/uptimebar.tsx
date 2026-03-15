import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';

export function UptimeBar({uptime}: {uptime?: number | null}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.uptime'});

    if (uptime == null) return null;

    const percent = Math.min(uptime, 100).toFixed(1);

    return (
        <>
            <br />
            <b>{t('title')}</b>
            <div style={{margin: '4px 0'}}>
                <div style={{background: '#eee', borderRadius: '4px', height: '16px', position: 'relative', overflow: 'hidden'}}>
                    <div
                        style={{
                            background: graphcolours[0],
                            height: '100%',
                            width: `${percent}%`,
                            borderRadius: '4px'
                        }}
                    />
                </div>
                <span style={{fontSize: '0.85rem'}}>{t('percent', {percent})}</span>
            </div>
        </>
    );
}
