import {LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, LabelList} from 'recharts';
import graphcolours from '../graphcolours';

import {WaitForGraph} from './waitforgraph';

export function GapDetails(props: //
{
    q: number;
    stationCount: number;
    p: number;
    byDay: any;
}) {
    return (
        <>
            <b>Time between received packets</b>
            <br />
            Avg Gap: {props.p >> 2}s{' '}
            {(props.q ?? true) !== true && props.stationCount > 1 ? (
                <>
                    (expected: {props.q >> 2}s)
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
                            <Line name="Average Gap" isAnimationActive={false} type="monotone" dataKey="avgGap" stroke={graphcolours[0]} dot={{r: 1}} />
                            {'expectedGap' in props.byDay[0] ? <Line name="Expected Gap" type="monotone" isAnimationActive={false} dataKey="expectedGap" stroke={graphcolours[1]} dot={{r: 1}} /> : null}
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
