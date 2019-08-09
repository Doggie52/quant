﻿using System;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

using QuantConnect.Logging;
using QuantConnect.Util;

using RestSharp;

namespace QuantConnect.Lean.Engine.DataFeeds
{
	public class Downloader
	{
		private string _url = "https://api-fxtrade.oanda.com/v3/instruments/{0}/candles?price={1}&from={2}&to={3}&granularity={4}";
		private string _token;
		private string _dataPath;
		private string _price = "BA";

		public void Initialize( string token, string dataPath )
		{
			_token = token;
			_dataPath = dataPath;
		}

		public bool DownloadData( Symbol symbol, Resolution resolution, DateTime fromDate )
		{
			if ( resolution == Resolution.Hour ) {
				return DownloadHourData( symbol, resolution, fromDate );
			}
			string r = "S5";
			string filename = "";
			string s = symbol.ID.Symbol.Insert( symbol.ID.Symbol.Length - 3, "_" );
			string url = null;
			DateTime toDate;
			switch ( resolution ) {
				case Resolution.Minute:
					r = "M1";
					filename = string.Format( "{0}{1:D2}{2:D2}_{3}_minute_quote.csv", new object[] { fromDate.Year, fromDate.Month, fromDate.Day, symbol.ID.Symbol } );
					toDate = fromDate.ToUniversalTime().AddDays( 1d );
					url = string.Format( _url, s, _price, ToUnixTimestamp( fromDate.ToUniversalTime() ), ToUnixTimestamp( toDate ), r );

					break;
				case Resolution.Second:
					r = "S5";
					filename = string.Format( "{0}{1:D2}{2:D2}_{3}_second_quote.csv", new object[] { fromDate.Year, fromDate.Month, fromDate.Day, symbol.ID.Symbol } );
					toDate = fromDate.ToUniversalTime().AddDays( 1d );
					url = string.Format( _url, s, _price, ToUnixTimestamp( fromDate.ToUniversalTime() ), ToUnixTimestamp( toDate ), r );
					break;
				case Resolution.Tick:
					r = "S5";
					filename = string.Format( "{0}{1:D2}{2:D2}_{3}_tick_quote.csv", new object[] { fromDate.Year, fromDate.Month, fromDate.Day, symbol.ID.Symbol } );
					toDate = fromDate.ToUniversalTime().AddDays( 1d );
					url = string.Format( _url, s, _price, ToUnixTimestamp( fromDate.ToUniversalTime() ), ToUnixTimestamp( toDate ), r );
					break;
				//case Resolution.Hour: {
				//		r = "H1";
				//		filename = string.Format( "{0}.csv", symbol.ID.Symbol );
				//		toDate = DateTime.UtcNow;
				//		url = string.Format( _url, s, _price, ToUnixTimestamp( fromDate.ToUniversalTime() ), ToUnixTimestamp( toDate ), r );
				//		break;
				//	}
				case Resolution.Daily: {
						r = "D";
						filename = string.Format( "{0}.csv", symbol.ID.Symbol );
						toDate = DateTime.UtcNow.Date.AddDays( -1 );
						url = string.Format( _url, s, _price, ToUnixTimestamp( fromDate.ToUniversalTime() ), ToUnixTimestamp( toDate ), r );
						break;

					}
			}

			RestClient client = new RestClient();
			var auth = "Bearer " + _token;
			client.Timeout = 120000;

			client.BaseUrl = new Uri( url );
			Log.Trace( string.Format( "Downloading {0}", url ) );
			var request = new RestRequest();
			request.AddHeader( "content-type", "application/json" );
			request.AddHeader( "Authorization", auth );
			IRestResponse response = client.Execute( request );
			string json = response.Content;
			if ( response.ErrorException == null ) {
				// Save csv in same folder heirarchy as Lean
				var path = Path.Combine( _dataPath, LeanData.GenerateRelativeZipFilePath( symbol.Value, symbol.ID.SecurityType, symbol.ID.Market, fromDate, resolution ) );

				// Make sure the directory exist before writing
				( new FileInfo( path ) ).Directory.Create();
				var csv = resolution == Resolution.Daily || resolution == Resolution.Hour ? JSonToCSV( json ) : JSonToCSV( fromDate, json );
				if ( csv == null || csv.Length < 1 ) {
					return false;
				}
				CreateZip( path, filename, csv );
				return true;
			}
			Log.Trace( string.Format( "Error downloading {0}", response.ErrorException ) );
			return false;
		}

		public bool DownloadHourData( Symbol symbol, Resolution resolution, DateTime fromDate )
		{
			string r = "S5";
			string filename = "";
			string s = symbol.ID.Symbol.Insert( symbol.ID.Symbol.Length - 3, "_" );
			DateTime toDate = DateTime.UtcNow.AddHours( -2 );
			string url = null;
			r = "H1";
			filename = string.Format( "{0}.csv", symbol.ID.Symbol.ToLower() );
			DateTime _d = fromDate.ToUniversalTime();
			string csv = "";
			while ( _d < toDate ) {
				DateTime _end = _d.AddHours( 5000 ) > toDate ? toDate : _d.AddHours( 5000 );
				url = string.Format( _url, s, _price, ToUnixTimestamp( _d ), ToUnixTimestamp( _end ), r );

				RestClient client = new RestClient();
				var auth = "Bearer " + _token;
				client.Timeout = 120000;

				client.BaseUrl = new Uri( url );
				Log.Trace( string.Format( "Downloading {0}", url ) );
				var request = new RestRequest();
				request.AddHeader( "content-type", "application/json" );
				request.AddHeader( "Authorization", auth );
				IRestResponse response = client.Execute( request );
				string json = response.Content;
				if ( response.ErrorException == null ) {
					var curcsv = resolution == Resolution.Daily || resolution == Resolution.Hour ? JSonToCSV( json ) : JSonToCSV( fromDate, json );
					if ( curcsv != null && curcsv.Length > 0 ) {
						csv += curcsv;
					}

				}
				_d = _end;
			}

			if ( csv.Length > 0 ) {
				// Save csv in same folder heirarchy as Lean
				var path = Path.Combine( _dataPath, LeanData.GenerateRelativeZipFilePath( symbol.Value, symbol.ID.SecurityType, symbol.ID.Market, fromDate, resolution ) );

				// Make sure the directory exist before writing
				( new FileInfo( path ) ).Directory.Create();
				CreateZip( path, filename, csv );
				return true;
			}
			return false;

		}

		/// <summary>
		/// Creates a zip containing a csv file with data.
		/// </summary>
		/// <param name="path">Full path (including file name) of zip file to create.</param>
		/// <param name="name">Full filename of csv file to create inside the zip file.</param>
		/// <param name="content">Content to populate the csv file with.</param>
		public void CreateZip( string path, string name, string content )
		{
			using ( var memoryStream = new MemoryStream() ) {
				using ( var archive = new ZipArchive( memoryStream, ZipArchiveMode.Create, true ) ) {
					var quotesFile = archive.CreateEntry( name );

					using ( var entryStream = quotesFile.Open() )
					using ( var streamWriter = new StreamWriter( entryStream ) ) {
						streamWriter.Write( content );
					}
				}

				using ( var fileStream = new FileStream( path, FileMode.Create ) ) {
					memoryStream.Seek( 0, SeekOrigin.Begin );
					memoryStream.CopyTo( fileStream );
				}
			}
		}

		private string JSonToCSV( string json )
		{

			string content = "";
			string row = "{0:yyyyMMdd HH:mm},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}";

			Instrument ins = Instrument.FromJson( json );
			if ( ins.Candles == null ) {
				return content;
			}
			foreach ( Candle candle in ins.Candles ) {
				content = content + string.Format( row, new object[] { candle.Time.ToUniversalTime(), candle.Bid.O, candle.Bid.H, candle.Bid.L, candle.Bid.C, candle.Volume, candle.Ask.O, candle.Ask.H, candle.Ask.L, candle.Ask.C, candle.Volume } ) + System.Environment.NewLine;
			}
			return content;
		}

		private string JSonToCSV( DateTime date, string json )
		{

			string content = "";
			string row = "{0},{1},{2},{3},{4},{5},{6},{7},{8},{9},{10}";

			Instrument ins = Instrument.FromJson( json );
			if ( ins.Candles == null ) {
				return content;
			}
			foreach ( Candle candle in ins.Candles ) {
				content = content + string.Format( row, new object[] { ( candle.Time.ToUniversalTime() - date.ToUniversalTime() ).TotalMilliseconds, candle.Bid.O, candle.Bid.H, candle.Bid.L, candle.Bid.C, candle.Volume, candle.Ask.O, candle.Ask.H, candle.Ask.L, candle.Ask.C, candle.Volume } ) + System.Environment.NewLine;
			}
			return content;
		}

		private int ToUnixTimestamp( DateTime dateTime )
		{
			return (int)( TimeZoneInfo.ConvertTimeToUtc( dateTime ) -
					 new DateTime( 1970, 1, 1, 0, 0, 0, 0, System.DateTimeKind.Utc ) ).TotalSeconds;
		}

		public static void Main( string[] args )
		{
			var d = new Downloader();

			d.Initialize( "token here", "Data" );

			var tickers = new string[] { "EURCHF", "EURGBP", "EURJPY", "EURUSD", "GBPCHF", "GBPJPY", "GBPUSD", "USDCAD", "USDCHF", "USDJPY", "GBPCAD" };

			Parallel.ForEach( tickers, ( ticker ) => {
				var symbol = QuantConnect.Symbol.Create( ticker, SecurityType.Forex, Market.Oanda );

				d.DownloadData( symbol, Resolution.Hour, new DateTime( 2008, 1, 1 ) );
			} );
		}
	}
}