import {useMemo} from 'react';
import {useTranslation} from 'next-i18next';
import {PieChart, Pie, Legend, Tooltip, ResponsiveContainer} from 'recharts';
import {layerFromDestCallsign} from '../../common/layers';
import graphcolours from '../graphcolours';
import type {ProtocolEntry} from '../../common/protocolstats';
import {layerColorForTocall} from './protocolstatsutil';

export function ProtocolTable({
    protocols,
    selectedTab,
    setLayers
}: {
    protocols: Record<string, ProtocolEntry>;
    selectedTab: string;
    setLayers: (l: string[]) => void;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});
    const {t: tProto} = useTranslation('common', {keyPrefix: 'protocols'});

    const entries = useMemo(() => {
        let list = Object.entries(protocols).map(([tocall, proto]) => ({
            tocall,
            layer: layerFromDestCallsign(tocall),
            accepted: proto.accepted,
            devices: proto.devices
        }));

        if (selectedTab !== 'all') {
            list = list.filter((e) => e.layer === selectedTab);
        }

        list.sort((a, b) => b.accepted - a.accepted);
        return list;
    }, [protocols, selectedTab]);

    if (entries.length <= 1) return null;

    const maxAccepted = entries[0]?.accepted ?? 1;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('protocol_table')}</b>
            <div style={{fontSize: 'small'}}>
                <table style={{width: '100%', borderCollapse: 'collapse'}}>
                    <thead>
                        <tr style={{fontSize: 'x-small', color: 'gray'}}>
                            <th style={{textAlign: 'left'}}></th>
                            <th style={{textAlign: 'right'}}>{t('accepted')}</th>
                            <th style={{textAlign: 'right'}}>{t('devices')}</th>
                        </tr>
                    </thead>
                    <tbody>
                        {entries.map((row) => {
                            const isCaptured = !!row.layer;
                            return (
                                <tr
                                    key={row.tocall}
                                    onClick={() => {
                                        if (isCaptured) setLayers([row.layer!]);
                                    }}
                                    style={{cursor: isCaptured ? 'pointer' : 'default', color: isCaptured ? undefined : 'gray'}}
                                    title={isCaptured ? t('click_to_filter') : undefined}
                                >
                                    <td style={{display: 'flex', alignItems: 'center', gap: '4px'}}>
                                        <span
                                            style={{
                                                display: 'inline-block',
                                                width: '8px',
                                                height: '8px',
                                                borderRadius: '50%',
                                                backgroundColor: layerColorForTocall(row.tocall),
                                                flexShrink: 0
                                            }}
                                        />
                                        <span>{tProto(row.tocall, row.tocall)}</span>
                                        <span
                                            style={{
                                                display: 'inline-block',
                                                height: '6px',
                                                backgroundColor: layerColorForTocall(row.tocall),
                                                opacity: 0.3,
                                                width: `${Math.max(1, (row.accepted / maxAccepted) * 60)}px`,
                                                flexShrink: 0
                                            }}
                                        />
                                    </td>
                                    <td style={{textAlign: 'right'}}>{row.accepted.toLocaleString()}</td>
                                    <td style={{textAlign: 'right'}}>{row.devices.toLocaleString()}</td>
                                </tr>
                            );
                        })}
                    </tbody>
                </table>
            </div>
        </>
    );
}

export function RegionPieChart({protocols, selectedTab}: {protocols: Record<string, ProtocolEntry>; selectedTab: string}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});
    const {t: tRegion} = useTranslation('common', {keyPrefix: 'regions'});

    const regionData = useMemo(() => {
        const regionTotals: Record<string, number> = {};
        for (const [tocall, proto] of Object.entries(protocols)) {
            if (selectedTab !== 'all') {
                if (layerFromDestCallsign(tocall) !== selectedTab) continue;
            }
            for (const [region, count] of Object.entries(proto.regions ?? {})) {
                regionTotals[region] = (regionTotals[region] ?? 0) + count;
            }
        }
        return Object.entries(regionTotals)
            .filter(([, v]) => v > 0)
            .sort(([, a], [, b]) => b - a)
            .map(([region, count], i) => ({
                name: tRegion(region, region),
                value: count,
                fill: graphcolours[i % graphcolours.length]
            }));
    }, [protocols, selectedTab, tRegion]);

    if (regionData.length === 0) return null;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('regions')}</b>
            <ResponsiveContainer width="100%" height={150}>
                <PieChart>
                    <Pie data={regionData} dataKey="value" nameKey="name" isAnimationActive={false} outerRadius={50} innerRadius={10} />
                    <Legend wrapperStyle={{fontSize: '0.7rem'}} />
                    <Tooltip />
                </PieChart>
            </ResponsiveContainer>
        </>
    );
}
