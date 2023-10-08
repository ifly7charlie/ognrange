import {useState, useCallback, useMemo} from 'react';
import useSWR from 'swr';
const fetcher = (args0, args1) => fetch(args0, args1).then((res) => res.json());

import {
    LineChart, //
    Line,
    XAxis,
    YAxis,
    CartesianGrid,
    Tooltip,
    Legend,
    ResponsiveContainer,
    PieChart,
    Pie,
    BarChart,
    Bar
} from 'recharts';

import {findIndex as _findIndex, reduce as _reduce, debounce as _debounce, map as _map} from 'lodash';

import {WaitForGraph} from './waitforgraph';
import graphcolours from '../graphcolours';

//
// Other detailed coverage for this cell
// COSTLY
export function OtherStationsDetails(props: {h3: string; file: string; station: string; locked: boolean}) {
    const [doFetch, setDoFetch] = useState(null);

    //
    // Get the grouped value - we use a delay for this so we don't hit server unless they wait
    const delayedUpdateFrom = useCallback(
        _debounce((x) => {
            setDoFetch(x);
        }, 500),
        [true]
    );

    const isGlobal = (props.station || 'global') == 'global'; //

    // What URL to use - both return same but act differently
    const url = () =>
        isGlobal //
            ? `/api/station/global/h3summary/${props.h3}?file=${props.file}&lockedH3=${props?.locked ? 1 : 0}`
            : `/api/station/${props.station}/h3others/${props.h3}?file=${props.file}&lockedH3=${props?.locked ? 1 : 0}`;

    // Actually load the data when it's time
    const {data: byDay} = useSWR(props.station + props?.h3 == doFetch && props?.h3 ? url() : null, fetcher);

    function safeZero(x) {
        return isNaN(x) ? 0 : x || 0;
    }

    const percentages = useMemo(() => {
        return _map(byDay?.data || [], (v) => {
            const dayTotal = _reduce(
                v,
                (r, count) => {
                    return (r = r + (typeof count == 'number' ? count : 0));
                },
                0
            );
            const newObject = {date: v['date']};
            for (const key of Object.keys(v)) {
                if (key != 'date') {
                    if (isNaN(v[key]) || v[key] == null) {
                        //                       delete v[key];
                    } else {
                        newObject[key] = Math.round((safeZero(v[key]) * 1000) / dayTotal) / 10;
                    }
                }
            }
            return newObject;
        });
    }, [props.h3, props.station, !!byDay]);

    delayedUpdateFrom(props.station + props.h3);

    return (
        <>
            {isGlobal ? <>Top 5 stations by percentage:</> : <>Top 5 other stations total packet count:</>}
            <br />
            {byDay?.data?.length > 0 ? (
                <>
                    <ResponsiveContainer width="100%" height={150} key="counttotal">
                        <PieChart width={480} height={180} margin={{top: 5, right: 30, left: 20, bottom: 5}}>
                            <Pie
                                data={_map(byDay.series, (v, i) => {
                                    return {...v, fill: graphcolours[i]};
                                })}
                                dataKey="c"
                                nameKey="s"
                                isAnimationActive={false}
                                outerRadius={50}
                                innerRadius={10}
                            />
                            <Legend />
                            <Tooltip />
                        </PieChart>
                    </ResponsiveContainer>
                    <br />
                    {!isGlobal ? (
                        <>
                            Top 5 other stations packet count by day:
                            <br />
                            <br />
                            <ResponsiveContainer width="100%" height={150} key="countovertime">
                                <LineChart width={480} height={180} data={byDay.data} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                                    <CartesianGrid strokeDasharray="3 3" />
                                    <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                                    <YAxis style={{fontSize: '0.8rem'}} />
                                    <Tooltip key="ttcot" />
                                    <Legend />
                                    {_map(byDay.series, (v, i) => (
                                        <Line key={v.s} isAnimationActive={false} type="monotone" dataKey={v.s} stroke={graphcolours[i]} dot={{r: 1}} />
                                    ))}
                                </LineChart>
                            </ResponsiveContainer>
                            <br />
                        </>
                    ) : null}
                    Top 5 other stations percentage by day:
                    <br />
                    <br />
                    <ResponsiveContainer width="100%" height={150} key="percentageovertime">
                        <BarChart width={480} height={180} data={percentages} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
                            <CartesianGrid strokeDasharray="3 3" />
                            <XAxis dataKey="date" style={{fontSize: '0.8rem'}} />
                            <YAxis unit="%" style={{fontSize: '0.8rem'}} allowDecimals={false} />
                            <Tooltip />
                            <Legend />
                            {_map(byDay.series, (v, i) => (
                                <Bar key={'ot' + v.s} isAnimationActive={false} type="monotone" stackId="a" dataKey={v.s} stroke={graphcolours[i]} fill={graphcolours[i]} />
                            ))}
                        </BarChart>
                    </ResponsiveContainer>
                </>
            ) : (
                <WaitForGraph />
            )}
            <hr />
        </>
    );
}
