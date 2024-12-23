using System;
using System.IO;
using System.Net.Http;
using System.Net;

namespace QuantConnect.DataSource
{
    /// <summary>
    /// Class for raw access of Dukascopy web site
    /// </summary>
    public class DukascopyRaw
    {
        private const string WebRoot = "http://www.dukascopy.com/datafeed";
        private static byte[] ba = Array.Empty<byte>();
        private static int ndx;

        /// <summary>
        /// Get next tick of Dukascopy stream
        /// </summary>
        /// <param name="hour"></param>
        /// <param name="symbol"></param>
        /// <param name="time"></param>
        /// <param name="ask"></param>
        /// <param name="bid"></param>
        /// <param name="askVolume"></param>
        /// <param name="bidVolume"></param>
        /// <param name="isFirstTickOfHour"></param>
        /// <param name="isLastTickOfHour"></param>
        /// <returns></returns>
        static public string GetNextQuote(DateTime hour,
           string symbol,
           out DateTime time,
           out uint ask,
           out uint bid,
           out float askVolume,
           out float bidVolume,
           out bool isFirstTickOfHour,
           out bool isLastTickOfHour)
        {
            isFirstTickOfHour = isLastTickOfHour = true;
            time = hour;
            askVolume = bidVolume = ask = bid = 0;
            string error = "", url = "";
            int retries;

            for (retries = 0; retries < 5; retries++)
            {
                try
                {
                    if (ndx >= ba.Length)
                    {
                        ndx = 0;
                        url = GetDukascopyFilename(DukascopyRaw.WebRoot + '/' + symbol, hour, '/');
                        using (var client = new HttpClient())
                        {
                            var stream = client.GetStreamAsync(url).Result;
                            ba = Decode(stream, out error).ToArray();
                            /* Dukascopy format see https://github.com/ninety47/dukascopy
                               The files I downloaded are named something like '00h_ticks.bi5'. These 'bi5' files are LZMA compressed binary data files. 
                               The binary data file are formatted into 20-byte rows.
                               32-bit integer: milliseconds since epoch
                               32-bit int: Ask price
                               32-bit int: Bid price
                               32-bit float: Ask volume
                               32-bit float: Bid volume
                               The ask and bid prices need to be multiplied by the point value for the symbol/currency pair.
                               The epoch is extracted from the URL (and the folder structure I've used to store the files on disk). 
                               It represents the point in time that the file starts from e.g. 2013/01/14/00h_ticks.bi5 has the epoch of midnight on 14 January 2013. */
                        }
                    }

                    if (0 == ba.Length)
                        return "";
                    else
                    {
                        time = hour + TimeSpan.FromMilliseconds(IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ba, ndx)));
                        ask = (uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ba, ndx + 4));
                        bid = (uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ba, ndx + 8));
                        askVolume = (BitConverter.ToSingle(BitConverter.GetBytes((uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ba, ndx + 12)))));
                        bidVolume = (BitConverter.ToSingle(BitConverter.GetBytes((uint)IPAddress.NetworkToHostOrder(BitConverter.ToInt32(ba, ndx + 16)))));
                        ndx += 20;
                    }
                    break;
                }
                catch { continue; }
            }

            if (5 == retries)
                return url + " not found";

            isFirstTickOfHour = 20 == ndx;
            isLastTickOfHour = ndx >= ba.Length;

            return "";
        }

        /// <summary>
        /// Fin first existing entry
        /// </summary>
        /// <param name="symbol"></param>
        /// <param name="endDate"></param>
        /// <returns></returns>
        static public string FindFirst(string symbol, out DateTime endDate)
        {
            var startDate = new DateTime(2000, 1, 1);
            endDate = DateTime.Now.Date;
            DateTime hour = DateTime.MinValue;
            do
            {
                try
                {
                    hour = new DateTime((endDate.Ticks + startDate.Ticks) / 2).Date;

                    var url = GetDukascopyFilename(WebRoot + '/' + symbol, hour, '/');
                    using (var client = new HttpClient())
                    {
                        var response = client.GetAsync(url).Result;
                        response.EnsureSuccessStatusCode();
                        var inStream = response.Content.ReadAsStreamAsync().Result;
                    }
                    endDate = hour;
                }
                catch
                {
                    startDate = hour;
                }
            } while (endDate - startDate > TimeSpan.FromDays(1));

            return "";
        }

        /// <summary>
        /// Reset current index to 0
        /// </summary>
        static public void ResetCurrentHour()
        {
            ndx = 0;
        }

        static private MemoryStream Decode(Stream inStream, out string error)
        {
            MemoryStream outStream = new MemoryStream();
            SevenZip.Compression.LZMA.Decoder decoder = new SevenZip.Compression.LZMA.Decoder();
            byte[] properties = new byte[5];

            error = null;
            if (0 != inStream.Read(properties, 0, 5))
            {
                decoder.SetDecoderProperties(properties);
                long outSize = 0;
                for (int i = 0; i < 8; i++)
                {
                    int v = inStream.ReadByte();
                    outSize |= ((long)(byte)v) << (8 * i);
                }
                try
                {
                    decoder.Code(inStream, outStream, 0, outSize, null);
                }
                catch (Exception e) { error = e.Message; }
            }
            return outStream;
        }

        static private string GetDukascopyFilename(string path, DateTime dt, char delimit, bool addCache = false)
        { // GBPUSD\\DukasCopy\\2007\\00\\30\\16h_ticks.bi5; month starting from 00, not from 01
            return
               path + delimit
               + (addCache ? "DukasCopy" + delimit : "")
               + dt.Year.ToString() + delimit
               + (dt.Month - 1).ToString("D2") + delimit
               + dt.Day.ToString("D2") + delimit
               + dt.Hour.ToString("D2") + "h_ticks.bi5";
        }
    }
}

