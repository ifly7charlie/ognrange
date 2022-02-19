
import { readFileSync } from 'fs';
import { tableFromIPC } from 'apache-arrow/Arrow.node';


import { DB_PATH, OUTPUT_PATH } from '../lib/bin/config.js';
import yargs from 'yargs';


const args = yargs(process.argv.slice(2))
	.option( 'station', 
			 { alias: 's',
			   type: 'string',
			   default: 'global',
			   description: 'Arrow file'
		})
	.option( 'file', 
			 { alias: 'f',
			   type: 'string',
			   default: '.year.arrow',
			   description: 'Arrow file'
		})
		.help()
		.alias( 'help', 'h' ).argv;



const arrow = readFileSync( OUTPUT_PATH + args.station + '/' + args.station + '.' +args.file );
const table = tableFromIPC([arrow]);

for( const columns of table ) {
//	console.log(
	let out = '';
	for( const column of columns ) {
		if( column[0] == 'h3' ) {
			out = (BigInt(column[1])).toString(16) + ' {';
		}
		else {
			out += `{"${column[0]}":${column[1]}},`;
		}
	}
	console.log( out.slice(0,-1) + '}' );
}

//console.table([...table].slice(0,100));

