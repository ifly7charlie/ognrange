
export function prefixWithZeros(len, number) {
	let prefix = '';
	for (let i = len-number.length; i > 0; i--) {
		prefix += '0';
	}
	return prefix+number;
}
