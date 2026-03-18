import {useState, useMemo} from 'react';
import useSWR from 'swr';
import {useTranslation} from 'next-i18next';

import {WaitForGraph} from './waitforgraph';
import {colorForTab} from './protocolstatsutil';
import {LayerTabs} from '../layertabs';
import {HourlyTrafficChart} from './protocolstatshourly';
import {AcceptedByDayChart, DevicesByDayChart} from './protocolstatsdaily';
import {ProtocolTable, RegionPieChart} from './protocolstatstable';
import type {ProtocolStatsApiResponse} from '../../common/protocolstats';
import {layerFromDestCallsign} from '../../common/layers';

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export function ProtocolStatsDashboard({layers, setLayers, dateRange}: {layers: string[]; setLayers: (l: string[]) => void; dateRange?: {start: string; end: string}}) {
    const {t} = useTranslation('common', {keyPrefix: 'stats'});

    const [selectedTab, setSelectedTab] = useState<string>('all');

    const statsUrl = useMemo(() => {
        const params = new URLSearchParams();
        if (dateRange?.start) params.set('dateStart', dateRange.start);
        if (dateRange?.end) params.set('dateEnd', dateRange.end);
        const qs = params.toString();
        return qs ? `/api/stats?${qs}` : '/api/stats';
    }, [dateRange?.start, dateRange?.end]);

    const {data} = useSWR<ProtocolStatsApiResponse>(statsUrl, fetcher);

    const tabs = useMemo(() => ['all', ...layers], [layers]);
    const color = useMemo(() => colorForTab(selectedTab), [selectedTab]);

    // Filter protocols to those matching the URL-selected layers.
    // Skip filtering when combined is selected — it aggregates all layers.
    const filteredProtocols = useMemo(() => {
        if (!data?.current?.protocols) return {};
        if (layers.includes('combined')) return data.current.protocols;
        const layerSet = new Set(layers);
        return Object.fromEntries(
            Object.entries(data.current.protocols).filter(([tocall]) => {
                const layer = layerFromDestCallsign(tocall);
                return layer === null || layerSet.has(layer);
            })
        );
    }, [data?.current?.protocols, layers]);

    if (!data) {
        return (
            <div>
                <b>{t('title')}</b>
                <br />
                <WaitForGraph />
            </div>
        );
    }

    if (!data.current) {
        return (
            <div>
                <b>{t('title')}</b>
                <br />
                <span style={{color: 'gray', fontStyle: 'italic'}}>{t('no_data')}</span>
            </div>
        );
    }

    const generatedTime = new Date(data.current.generated).toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
    const startDate = new Date(data.current.startTime).toLocaleDateString([], {month: 'short', day: 'numeric'});
    const endDate = new Date(data.current.generated).toLocaleDateString([], {month: 'short', day: 'numeric'});
    const period = startDate === endDate ? startDate : `${startDate} – ${endDate}`;

    return (
        <div>
            <b>{t('title')}</b>
            <br />
            <span style={{fontSize: 'small', color: 'gray'}}>
                {period} · {t('generated', {time: generatedTime})}
            </span>
            <br />

            <LayerTabs tabs={tabs} selectedTab={selectedTab} setSelectedTab={setSelectedTab} />

            <HourlyTrafficChart data={data} selectedTab={selectedTab} color={color} />
            {data.dailyDevices.length > 0 && (
                <>
                    <AcceptedByDayChart dailyDevices={data.dailyDevices} selectedTab={selectedTab} color={color} />
                    <DevicesByDayChart dailyDevices={data.dailyDevices} selectedTab={selectedTab} color={color} />
                </>
            )}
            <RegionPieChart protocols={filteredProtocols} selectedTab={selectedTab} />
            <ProtocolTable protocols={filteredProtocols} selectedTab={selectedTab} setLayers={setLayers} period={period} isRange={!data.devicesExact} />
        </div>
    );
}
