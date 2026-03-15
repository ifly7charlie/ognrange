'use client';

import {useTranslation} from 'next-i18next';
import {colorForTab} from './coveragedetails/protocolstatsutil';

export function LayerTabs({
    tabs,
    selectedTab,
    setSelectedTab,
    hasData
}: {
    tabs: string[];
    selectedTab: string;
    setSelectedTab: (tab: string) => void;
    hasData?: (tab: string) => boolean;
}) {
    const {t: tLayer} = useTranslation('common', {keyPrefix: 'layers'});

    return (
        <div style={{margin: '4px 0', display: 'flex', flexWrap: 'wrap', gap: '2px'}}>
            {tabs.map((tab) => {
                const isSelected = selectedTab === tab;
                const available = !hasData || hasData(tab);
                return (
                    <button
                        key={tab}
                        onClick={() => setSelectedTab(tab)}
                        style={{
                            fontWeight: isSelected ? 'bold' : 'normal',
                            color: isSelected ? 'white' : available ? undefined : 'gray',
                            backgroundColor: isSelected ? colorForTab(tab) : undefined,
                            border: '1px solid #ccc',
                            borderRadius: '4px',
                            padding: '2px 8px',
                            cursor: 'pointer'
                        }}
                    >
                        {tLayer(tab, tab)}
                    </button>
                );
            })}
        </div>
    );
}
