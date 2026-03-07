const zeroString = '00000000000000000000000000000000000000000000000000000000000000000000000000000000000';

export function prefixWithZeros(len: number, number: string): string {
    const pad = len - number.length;
    return pad > 0 ? zeroString.slice(0, pad) + number : number;
}
