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

import {findIndex as _findIndex, reduce as _reduce, debounce as _debounce, map as _map} from 'lodash';

import {WaitForGraph} from './waitforgraph';
import graphcolours from '../graphcolours';

export function SignalDetails(props: {a: number; e: number; byDay: any}) {
    return (
        <>
            <b>Signal Strength</b>
            <br />
            Average: {(props.a / 4).toFixed(1)} dB, Max: {(props.e / 4).toFixed(1)} dB
            <br />
            {props.byDay && props.byDay.length > 0 ? (
                <>
                    <br />
                    <ResponsiveContainer width="100%" height={150}>
                        <LineChart width={480} height={180} data={props.byDay} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="dB" style={{fontSize: '0.8rem'}} />
                            <Tooltip />
                            <Legend />
                            <Line name="Average" isAnimationActive={false} type="monotone" dataKey="avgSig" stroke={graphcolours[0]} dot={{r: 1}} />
                            <Line name="Maximum" isAnimationActive={false} type="monotone" dataKey="maxSig" stroke={graphcolours[1]} dot={{r: 1}} />
                            <Line name="At Minimum Altitude" isAnimationActive={false} type="monotone" dataKey="minAltSig" stroke={graphcolours[2]} dot={{r: 1}} />
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
