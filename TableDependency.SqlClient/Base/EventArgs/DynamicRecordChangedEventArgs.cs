#region License
// TableDependency, SqlTableDependency
// Copyright (c) 2015-2020 Christian Del Bianco. All rights reserved.
//
// Permission is hereby granted, free of charge, to any person
// obtaining a copy of this software and associated documentation
// files (the "Software"), to deal in the Software without
// restriction, including without limitation the rights to use,
// copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following
// conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
// OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
// HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
// WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
// FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
// OTHER DEALINGS IN THE SOFTWARE.
#endregion

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.Messages;

namespace TableDependency.SqlClient.Base.EventArgs
{
	public class DynamicRecordChangedEventArgs : BaseEventArgs
	{
		#region Instance variables

		protected MessagesBag MessagesBag { get; }

		#endregion Instance variables

		#region Properties

		public IDictionary<string, object> ColumnValues { get; protected set; }
		public IDictionary<string, object> ColumnOldValues { get; protected set; }
		public ChangeType ChangeType { get; protected set; }

		#endregion Properties

		#region Constructors

		public DynamicRecordChangedEventArgs(
			MessagesBag messagesBag,
			string server,
			string database,
			string sender,
			CultureInfo cultureInfo,
			bool includeOldValues = false) : base(server, database, sender, cultureInfo)
		{
			this.MessagesBag = messagesBag;

			this.ChangeType = messagesBag.MessageType;

			this.ColumnValues = this.MaterializeEntity(messagesBag.Messages.Where(m => !m.IsOldValue).ToList());

			if (includeOldValues && this.ChangeType == ChangeType.Update)
			{
				this.ColumnOldValues = this.MaterializeEntity(messagesBag.Messages.Where(m => m.IsOldValue).ToList());
			}
			else
			{
				this.ColumnOldValues = new Dictionary<string, object>();
			}
		}

		#endregion Constructors

		protected virtual IDictionary<string, object> MaterializeEntity(List<Message> messages)
		{
			var row = new Dictionary<string, object>();
			foreach (var message in messages)
			{
				var stringValue = message.Body == null ? "null" : Convert.ToString(this.MessagesBag.Encoding.GetString(message.Body), base.CultureInfo);
				row.Add(message.Recipient, stringValue);
			}

			return row;
		}
	}
}