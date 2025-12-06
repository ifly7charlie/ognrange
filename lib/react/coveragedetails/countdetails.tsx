import {
    LineChart, //
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer
} from 'recharts';

import {useTranslation} from 'next-i18next';

import {findIndex as _findIndex, reduce as _reduce, debounce as _debounce, map as _map} from 'lodash';

import graphcolours from '../graphcolours';

export function CountDetails(props: {byDay: any; c: number}) {
    const {t} = useTranslation();
    return (
        <>
            {t('details.packets.summary', {count: props.c})}
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name={t('details.packets.count')} isAnimationActive={false} type="monotone" dataKey="count" stroke={graphcolours[0]} dot={{r: 1}} />
                        </LineChart>
                    </ResponsiveContainer>
                </>
            ) : null}
        </>
    );
}
