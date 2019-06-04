using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;

using QuantConnect.Lean.Engine.DataFeeds;
using QuantConnect.Logging;
using System.Globalization;

namespace OandaDataProvider.Utils
{
	/// <summary>
	/// Provides functionality to merge two csvs together.
	/// </summary>
	/// <remarks>
	/// Base: the base csv (often QuantConnect's data zip).
	/// Overwriter: the better quality or more complete csv (often OANDA's data zip) which will overwrite any duplicates.
	/// </remarks>
	public class Merger
	{

		private static readonly DirectoryInfo _baseDir = new DirectoryInfo( "MergeBase" );

		private static readonly DirectoryInfo _overwriterDir = new DirectoryInfo( "MergeOverwriter" );

		private static readonly DirectoryInfo _resultDir = new DirectoryInfo( "MergeResult" );

		/// <summary>
		/// Merges base and overwriter zips together.
		/// </summary>
		public static void Main( string[] args )
		{

			// Base and overwriter directories must exist
			if ( !_baseDir.Exists || !_overwriterDir.Exists )
				throw new Exception( $"Base or overwriter zip directories do not exist, please create them as {_baseDir} and {_overwriterDir}." );

			// Create result directory if it doesn't already exist
			_resultDir.Create();

			// Get all overwrite files
			string[] overwriters = Directory.GetFiles( _overwriterDir.ToString(), "*.csv" );

			// Loop over all overwriters
			foreach ( string overwriterPath in overwriters ) {

				string overwriterFileName = new FileInfo( overwriterPath ).Name;
				string overwriterName = overwriterFileName.Split( '.' )[0];

				Log.Trace( $"Merging {overwriterFileName}." );

				// Read overwriter
				var overwriterRows = new Dictionary<string, string>();
				using ( var reader = new StreamReader( overwriterPath ) ) {
					while ( !reader.EndOfStream ) {
						var row = reader.ReadLine().Split( ',' );
						overwriterRows.Add( row[0], String.Join( ",", row ) );
					}
				}

				// Do we have a base?
				string basePath = Path.Combine( _baseDir.ToString(), overwriterFileName );
				if ( !File.Exists( basePath ) )
					throw new Exception( $"Base file not found at {basePath}." );

				// Read base
				var baseRows = new Dictionary<string, string>();
				using ( var reader = new StreamReader( basePath ) ) {
					while ( !reader.EndOfStream ) {
						var row = reader.ReadLine().Split( ',' );
						baseRows.Add( row[0], String.Join( ",", row ) );
					}
				}

				// Build final result from left join of overwriter and base
				var resultRows = new Dictionary<string, string>();
				foreach ( var baseRow in baseRows )
					resultRows[baseRow.Key] = baseRow.Value;
				foreach ( var overwriterRow in overwriterRows )
					resultRows[overwriterRow.Key] = overwriterRow.Value;

				// Sort results - a bit brutish but we want to ensure we don't mess this up
				var resultsRowsSorted = resultRows.ToList();
				resultsRowsSorted.Sort( ( x, y ) => DateTime.Compare( DateTime.ParseExact( x.Key, "yyyyMMdd HH:mm", CultureInfo.CurrentCulture ), DateTime.ParseExact( y.Key, "yyyyMMdd HH:mm", CultureInfo.CurrentCulture ) ) );

				// Append to CSV
				var resultCsv = new StringBuilder();
				foreach ( var row in resultsRowsSorted ) {
					resultCsv.Append( row.Value );
					resultCsv.AppendLine();
				}

				// Store result to zip in correct dir
				var dl = new Downloader();
				dl.CreateZip( Path.Combine( _resultDir.ToString(), overwriterName + ".zip" ), overwriterFileName, resultCsv.ToString() );
			}

		}
	}
}
