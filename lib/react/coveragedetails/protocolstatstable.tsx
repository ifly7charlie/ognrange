import {useMemo} from 'react';

type ProtocolRowEntry = {tocall: string; layer: string | null; accepted: number; devices: number};

function ProtocolRow({row, maxAccepted, setLayers}: {row: ProtocolRowEntry; maxAccepted: number; setLayers?: (l: string[]) => void}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});
    const {t: tProto} = useTranslation('common', {keyPrefix: 'protocols'});
    const isCaptured = !!row.layer && !!setLayers;
    return (
        <tr
            onClick={isCaptured ? () => setLayers!([row.layer!]) : undefined}
            style={{cursor: isCaptured ? 'pointer' : 'default', color: row.layer ? undefined : 'gray'}}
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
                <span style={{display: 'inline-block', width: '60px', flexShrink: 0}}>
                    <span
                        style={{
                            display: 'block',
                            height: '6px',
                            backgroundColor: layerColorForTocall(row.tocall),
                            opacity: 0.3,
                            width: `${Math.max(1, (row.accepted / maxAccepted) * 100)}%`,
                            borderRadius: '2px'
                        }}
                    />
                </span>
                <span>{tProto(row.tocall, row.tocall)}</span>
            </td>
            <td style={{textAlign: 'right'}}>{row.accepted.toLocaleString()}</td>
            <td style={{textAlign: 'right'}}>{row.devices.toLocaleString()}</td>
        </tr>
    );
}
import {useTranslation} from 'next-i18next';
import {PieChart, Pie, Legend, Tooltip, ResponsiveContainer} from 'recharts';
import {layerFromDestCallsign} from '../../common/layers';
import graphcolours from '../graphcolours';
import type {ProtocolEntry} from '../../common/protocolstats';
import {layerColorForTocall} from './protocolstatsutil';

export function ProtocolTable({
    protocols,
    selectedTab,
    setLayers,
    period,
    isRange
}: {
    protocols: Record<string, ProtocolEntry>;
    selectedTab: string;
    setLayers: (l: string[]) => void;
    period?: string;
    /** True when the data spans multiple days — shows "Avg Devices" header */
    isRange?: boolean;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const {supported, unsupported} = useMemo(() => {
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
        return {
            supported: list.filter((e) => e.layer !== null),
            unsupported: list.filter((e) => e.layer === null)
        };
    }, [protocols, selectedTab]);

    if (supported.length + unsupported.length <= 1) return null;

    const maxAccepted = supported[0]?.accepted ?? unsupported[0]?.accepted ?? 1;

    return (
        <>
            <b style={{fontSize: 'small'}}>{t('protocol_table')}</b>
            {period && <span style={{fontSize: 'x-small', color: 'gray'}}> · {period}</span>}
            <div style={{fontSize: 'small'}}>
                <table style={{width: '100%', borderCollapse: 'collapse'}}>
                    <thead>
                        <tr style={{fontSize: 'x-small', color: 'gray'}}>
                            <th style={{textAlign: 'left'}}></th>
                            <th style={{textAlign: 'right'}}>{t('accepted')}</th>
                            <th style={{textAlign: 'right'}}>{isRange ? t('devices_avg') : t('devices')}</th>
                        </tr>
                    </thead>
                    <tbody>
                        {supported.map((row) => <ProtocolRow key={row.tocall} row={row} maxAccepted={maxAccepted} setLayers={setLayers} />)}
                    </tbody>
                </table>
                {unsupported.length > 0 && (
                    <details style={{marginTop: '4px'}}>
                        <summary style={{fontSize: 'x-small', color: 'gray', cursor: 'pointer'}}>
                            {t('unsupported', {count: unsupported.length})}
                        </summary>
                        <table style={{width: '100%', borderCollapse: 'collapse'}}>
                            <tbody>
                                {unsupported.map((row) => <ProtocolRow key={row.tocall} row={row} maxAccepted={maxAccepted} />)}
                            </tbody>
                        </table>
                    </details>
                )}
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
