using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SimulationConsole
{
    public class RunSettings
    {
        #region Properties
        public Uri SourceBlobPrefixUri { get; }
        
        public int? SourceCount { get; }
        #endregion

        #region Constructors
        public static RunSettings FromEnvironmentVariables()
        {
            var sourceBlobPrefixUri = GetUri("SourceBlobPrefix");
            var sourceCount = GetInt("SourceCount", false);

            return new RunSettings(sourceBlobPrefixUri, sourceCount);
        }

        public RunSettings(Uri sourceBlobPrefixUri, int? sourceCount)
        {
            SourceBlobPrefixUri = sourceBlobPrefixUri;
            SourceCount = sourceCount;
        }
        #endregion

        #region Environment variables
        #region String
        private static string GetString(string variableName)
        {
            var value = GetString(variableName, true);

            return value!;
        }

        private static string? GetString(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            return value;
        }
        #endregion

        #region Uri
        private static Uri GetUri(string variableName)
        {
            var uri = GetUri(variableName, true);

            return uri!;
        }

        private static Uri? GetUri(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                try
                {
                    var uri = new Uri(value, UriKind.Absolute);

                    return uri;
                }
                catch
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Enum
        private static T? GetEnum<T>(string variableName, bool mustExist)
            where T : struct
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (Enum.TryParse<T>(value, out var enumValue))
                {
                    return enumValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Bool
        private static bool? GetBool(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (bool.TryParse(value, out var boolValue))
                {
                    return boolValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion

        #region Int
        private static int? GetInt(string variableName, bool mustExist)
        {
            var value = Environment.GetEnvironmentVariable(variableName);

            if (mustExist && value == null)
            {
                throw new ArgumentNullException(variableName, "Environment variable missing");
            }

            if (value != null)
            {
                if (int.TryParse(value, out var boolValue))
                {
                    return boolValue;
                }
                else
                {
                    throw new ArgumentNullException(variableName, $"Unsupported value:  '{value}'");
                }
            }
            else
            {
                return null;
            }
        }
        #endregion
        #endregion

        public void WriteOutSettings()
        {
            Console.WriteLine();
            Console.WriteLine($"SourceBlobPrefixUri:  {SourceBlobPrefixUri}");
            Console.WriteLine();
        }
    }
}