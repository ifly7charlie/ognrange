const zeroString = '00000000000000000000000000000000000000000000000000000000000000000000000000000000000';

export function prefixWithZeros(len: number, number: string): string {
    return zeroString.slice(0, len - number.length) + number;
}
