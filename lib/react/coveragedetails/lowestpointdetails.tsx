import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';
import graphcolours from '../graphcolours';

import {useTranslation} from 'next-i18next';

import {WaitForGraph} from './waitforgraph';

export function LowestPointDetails(props: {d: number; b: number; g: number; byDay: any}) {
    const {t} = useTranslation('common', {keyPrefix: 'details.lowest'});
    return (
        <>
            <b>{t('title')}</b>
            <br />
            {t('summary', {strength: (props.d / 4).toFixed(1), altitude: props.b, agl: props.g})}
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="dB" yAxisId={0} orientation="left" style={{fontSize: '0.8rem'}} />
                            <YAxis unit=" m" yAxisId={1} orientation="right" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name={t('strength')} isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke={graphcolours[0]} dot={{r: 1}} />
                            <Line name={t('agl')} yAxisId={1} isAnimationActive={false} type="monotone" dataKey="minAgl" stroke={graphcolours[1]} dot={{r: 1}} />
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
