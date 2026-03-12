import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer} from 'recharts';

import {useTranslation} from 'next-i18next';

import graphcolours from '../graphcolours';
import {formatEpoch, formatEpochDateOnly} from '../formatdate';

import type {RollupActivity} from '../../worker/rollupactivity';

function formatDuration(startEpoch: number, endEpoch: number): string {
    const hours = Math.round((endEpoch - startEpoch) / 3600);
    if (hours < 24) return `${hours}h`;
    const days = Math.round(hours / 24);
    return `${days}d`;
}

export function ActivityDetails(props: {activity: RollupActivity | undefined}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.activity'});

    if (!props.activity) return null;

    const a = props.activity;
    const percent = a.totalRollups > 0 ? ((a.activeRollups / a.totalRollups) * 100).toFixed(1) : '0';

    const chartData = a.ranges.map((r) => ({
        date: formatEpochDateOnly(r.start),
        cells: r.cells
    }));

    return (
        <>
            <br />
            <b>{t('title')}</b>
            <br />
            <div style={{margin: '4px 0'}}>
                <div style={{background: '#eee', borderRadius: '4px', height: '16px', position: 'relative', overflow: 'hidden'}}>
                    <div
                        style={{
                            background: graphcolours[0],
                            height: '100%',
                            width: `${Math.min(parseFloat(percent), 100)}%`,
                            borderRadius: '4px'
                        }}
                    />
                </div>
                <span style={{fontSize: '0.85rem'}}>{t('active', {percent})}</span>
            </div>

            {chartData.length > 1 ? (
                <ResponsiveContainer width="100%" height={150}>
                    <LineChart data={chartData} margin={{top: 5, right: 30, left: 20, bottom: 5}}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                        <YAxis style={{fontSize: '0.8rem'}} />
                        <Tooltip />
                        <Line isAnimationActive={false} type="monotone" dataKey="cells" stroke={graphcolours[0]} dot={{r: 1}} />
                    </LineChart>
                </ResponsiveContainer>
            ) : null}

            <table>
                <tbody>
                    {a.firstSeen ? (
                        <tr>
                            <td>{t('firstSeen')}</td>
                            <td>{formatEpoch(a.firstSeen)}</td>
                        </tr>
                    ) : null}
                    {a.lastSeen ? (
                        <tr>
                            <td>{t('lastSeen')}</td>
                            <td>{formatEpoch(a.lastSeen)}</td>
                        </tr>
                    ) : null}
                    {a.lastRollup ? (
                        <tr>
                            <td>{t('lastRollup')}</td>
                            <td>{formatEpoch(a.lastRollup)}</td>
                        </tr>
                    ) : null}
                    <tr>
                        <td>{t('activeRanges', {count: a.ranges.length})}</td>
                        <td />
                    </tr>
                </tbody>
            </table>

            {a.ranges.length > 0 ? (
                <>
                    <br />
                    <b>{t('periods')}</b>
                    <div style={{maxHeight: '200px', overflowY: 'auto'}}>
                        <table style={{fontSize: '0.85rem', width: '100%'}}>
                            <thead>
                                <tr>
                                    <th style={{textAlign: 'left'}}>Start</th>
                                    <th style={{textAlign: 'left'}}>End</th>
                                    <th style={{textAlign: 'right'}}>Duration</th>
                                    <th style={{textAlign: 'right'}}>Cells</th>
                                </tr>
                            </thead>
                            <tbody>
                                {a.ranges.map((r, i) => (
                                    <tr key={i}>
                                        <td>{formatEpochDateOnly(r.start)}</td>
                                        <td>{formatEpochDateOnly(r.end)}</td>
                                        <td style={{textAlign: 'right'}}>{formatDuration(r.start, r.end)}</td>
                                        <td style={{textAlign: 'right'}}>{r.cells}</td>
                                    </tr>
                                ))}
                            </tbody>
                        </table>
                    </div>
                </>
            ) : null}
            <hr />
        </>
    );
}
