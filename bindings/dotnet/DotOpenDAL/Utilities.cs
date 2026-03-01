/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

using System.Runtime.InteropServices;
using System.Globalization;

namespace DotOpenDAL;

public static class Utilities
{
	/// <summary>
	/// Decodes an unmanaged UTF-8 message pointer into managed text.
	/// </summary>
	/// <param name="message">Pointer to an unmanaged UTF-8 message buffer.</param>
	/// <returns>The decoded message, or an empty string when the pointer is null or decoding returns null.</returns>
	public static string DecodeUtf8Message(IntPtr message)
	{
		if (message == IntPtr.Zero)
		{
			return string.Empty;
		}

		return Marshal.PtrToStringUTF8(message) ?? string.Empty;
	}

	/// <summary>
	/// Formats a managed value into the option string expected by OpenDAL service configs.
	/// </summary>
	/// <param name="value">Managed value to format.</param>
	/// <returns>The formatted option string.</returns>
	public static string ToOptionString(object value)
	{
		return value switch
		{
			bool b => b ? "true" : "false",
			IFormattable f => f.ToString(null, CultureInfo.InvariantCulture) ?? string.Empty,
			_ => value.ToString() ?? string.Empty,
		};
	}
}