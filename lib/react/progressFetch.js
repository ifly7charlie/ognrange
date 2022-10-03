/* MIT License
 * 
 * Copyright (c) 2018 Anthum, Inc
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE. 

*
* This code is based primarily on 
* https://github.com/AnthumChris/fetch-progress-indicators/blob/efaaaf073bc6927a803e5963a92ba9b11a585cc0/fetch-basic/supported-browser.js
*
* To get it to work with @loaders.gl I needed to preserve the headers 
*
 */

let reader = null;
export function progressFetch(setProgress) {
    return (r) => fetchProgressReader(r, setProgress);
}

function fetchProgressReader(response, setProgress) {
    if (!response.ok) {
        throw Error(response.status + ' ' + response.statusText);
    }

    if (!response.body) {
        throw Error('ReadableStream not yet supported in this browser.');
    }

    // Only one request allowed at a time so we will kill off the previous one
    // this is very much specific to this app!
    if (reader) {
        reader.cancel();
        reader = null;
    }

    // to access headers, server must send CORS header "Access-Control-Expose-Headers: content-encoding, content-length x-file-size"
    // server must send custom x-file-size header if gzip or other content-encoding is used
    const contentEncoding = response.headers.get('content-encoding');
    let contentLength;
    if (response.headers.get('content-length')) {
        contentLength = response.headers.get('content-length') || 500000;
    } else {
        contentLength = parseInt(response.headers.get('etag').split('-').slice(-2, -1), 16) * 8;
    }
    if (contentLength === null) {
        throw Error('Response size header unavailable');
    }

    const total = parseInt(contentLength, 10) * 2.7; // we are achieving about this as a compression ratio
    let loaded = 0;

    return new Response(
        new ReadableStream({
            start(controller) {
                reader = response.body.getReader();

                read();
                function read() {
                    reader
                        .read()
                        .then(({done, value}) => {
                            if (done) {
                                controller.close();
                                setProgress(null);
                                return;
                            }
                            //                            console.log(value.byteLength);
                            loaded += value.byteLength;
                            setProgress(loaded / total);
                            controller.enqueue(value);
                            read();
                        })
                        .catch((error) => {
                            console.error(error);
                            controller.error(error);
                            setProgress(null);
                        });
                }
            }
        }),
        {headers: new Headers(response.headers), status: response.status, statusText: response.statusText}
    );
}
