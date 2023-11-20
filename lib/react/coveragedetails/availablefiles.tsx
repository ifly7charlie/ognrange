import {BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer} from 'recharts';
import graphcolours from '../graphcolours';

import {WaitForGraph} from './waitforgraph';

import {useMemo, useCallback} from 'react';
import useSWR from 'swr';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

function getDaysBetween(start: string, end: string): Map<number, 0 | 1> {
    let result = new Map<number, 0 | 1>();
    const endDate = new Date(end + 'T00:00:00Z');
    for (let c = new Date(start + 'T00:00:00Z'); c <= endDate; c.setUTCDate(c.getUTCDate() + 1)) {
        result.set(c.valueOf(), 0);
    }
    return result;
}

const tickFormatter = {
    //
    day: (tick: number) => new Date(tick).toJSON().substring(5, 10).replace('-', '/'),
    month: (tick: number) => new Date(tick).toJSON().substring(0, 7).replace('-', '/'),
    year: (tick: number) => new Date(tick).toJSON().substring(0, 4)
};

const fileFormatter = {
    //
    day: (tick: number) => 'day.' + new Date(tick).toJSON().substring(0, 10),
    month: (tick: number) => 'month.' + new Date(tick).toJSON().substring(0, 7),
    year: (tick: number) => 'year.' + new Date(tick).toJSON().substring(0, 4)
};

const suffix = {
    day: 'T00:00:00Z',
    month: '-01T00:00:00Z',
    year: '-01-01T00:00:00Z'
};

//const yTickFormatter = (tick: number) => (tick ? 'available' : '');

const CustomTooltip = ({active, payload, label, tickFormatter}: {active?: any; payload?: any; label?: number; tickFormatter: (_a: number) => string}) => {
    if (active && payload && payload.length) {
        return (
            <div className="tooltip" style={{background: 'white', paddingLeft: '10px', paddingRight: '10px', border: '1px solid grey'}}>
                <p className="label">
                    <b>{tickFormatter(label)}</b>: {payload[0].value ? 'available' : ''}
                </p>
            </div>
        );
    }

    return null;
};

export function AvailableFiles({
    station,
    displayType = 'day',
    setFile
}: //
{
    station: string;
    displayType: 'day' | 'month' | 'year';
    setFile: (file: string) => void;
}) {
    const {data} = useSWR(`/api/station/${station || 'global'}`, fetcher);

    // Find the available range and then produce a set with flags indicating if it
    // is actually available
    const all = data?.files?.[displayType]?.all;
    const processed = useMemo(
        () =>
            all?.reduce((out: Map<number, 0 | 1>, fileName: string) => {
                out.set(new Date((fileName + suffix[displayType]).slice(-20)).valueOf(), 1);
                return out;
            }, getDaysBetween(all[0].slice(-10), all[all.length - 1].slice(-10))),
        [station, data]
    );

    // Conver this into something that can be displayed by the graph
    const processedArray = useMemo(() => (processed ? [...processed.entries()].map(([key, value]) => ({date: key, available: value})) : []), [processed]);

    // If the user clicks then jumpt to that file
    const onClick = useCallback(
        (data: {date: number}) => {
            setFile(fileFormatter[displayType](data.date));
        },
        [displayType, station]
    );

    if (!all || all.length === 1) {
        return null;
    }

    return (
        <>
            <b>{displayType}s available</b>
            {data ? (
                <ResponsiveContainer width="100%" height={75}>
                    <BarChart width={480} height={180} data={processedArray} margin={{top: 5, right: 30, left: 0, bottom: 5}} syncId={'date' + displayType}>
                        <CartesianGrid strokeDasharray="3 3" />
                        <XAxis dataKey="date" style={{fontSize: '0.8rem'}} tickFormatter={tickFormatter[displayType]} />

                        <Tooltip content={<CustomTooltip tickFormatter={tickFormatter[displayType]} />} />
                        <Bar key="Available" isAnimationActive={false} type="monotone" dataKey="available" stroke={graphcolours[0]} fill={graphcolours[0]} onClick={onClick} />
                    </BarChart>
                </ResponsiveContainer>
            ) : (
                <WaitForGraph />
            )}
        </>
    );
}

//                    <LineChart width={480} height={180} data={data} margin={{top: 5, right: 30, left: 20, bottom: 5}} syncId="date">
//                        <Line name="Available" yAxisId={0} isAnimationActive={false} type="monotone" dataKey="a" stroke={graphcolours[1]} dot={{r: 1}} />
