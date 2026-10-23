const dateFmt = new Intl.DateTimeFormat(undefined, {dateStyle: 'medium', timeStyle: 'short', timeZone: 'UTC'});
const dateOnlyFmt = new Intl.DateTimeFormat(undefined, {dateStyle: 'medium', timeZone: 'UTC'});

export function formatEpoch(epoch: number): string {
    return dateFmt.format(new Date(epoch * 1000)) + ' UTC';
}

export function formatEpochDateOnly(epoch: number): string {
    return dateOnlyFmt.format(new Date(epoch * 1000));
}
