import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';
import graphcolours from '../graphcolours';

import {useTranslation} from 'next-i18next';

import {WaitForGraph} from './waitforgraph';

export function GapDetails(props: //
{
    q: number;
    stationCount: number;
    p: number;
    byDay: any;
}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.gap'});
    return (
        <>
            <b>{t('title')}</b>
            <br />
            {t('summary', {average: props.p >> 2})}{' '}
            {(props.q ?? true) !== true && props.stationCount > 1 ? (
                <>
                    {t('summary_expected', {expected: props.q >> 2})}
                    <br />
                </>
            ) : (
                <br />
            )}
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="s" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name={t('average')} isAnimationActive={false} type="monotone" dataKey="avgGap" stroke={graphcolours[0]} dot={{r: 1}} />
                            {'expectedGap' in props.byDay[0] ? <Line name={t('expected')} type="monotone" isAnimationActive={false} dataKey="expectedGap" stroke={graphcolours[1]} dot={{r: 1}} /> : null}
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : (
                <WaitForGraph />
            )}
            <hr />
        </>
    );
}
