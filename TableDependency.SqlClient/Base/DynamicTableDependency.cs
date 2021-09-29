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
using System.ComponentModel.DataAnnotations.Schema;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Reflection;

using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Delegates;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Messages;
namespace TableDependency.SqlClient.Base
{
	public abstract class DynamicTableDependency : BaseTableDependency, IDynamicTableDependency
	{
		#region Events

		/// <summary>
		/// Occurs when the table content has been changed with an update, insert or delete operation.
		/// </summary>
		public abstract event ChangedEventHandler OnChanged;

		#endregion Events

		#region Constructors

		protected DynamicTableDependency(
			string connectionString,
			string tableName = null,
			string schemaName = null,
			ITableDependencyFilter filter = null,
			DmlTriggerType dmlTriggerType = DmlTriggerType.All,
			bool executeUserPermissionCheck = true) : base(connectionString, tableName, schemaName, filter, dmlTriggerType, executeUserPermissionCheck)
		{
			var tableColumnList = this.GetTableColumnsList();
			if (!tableColumnList.Any()) throw new TableWithNoColumnsException(_tableName);

			this.CheckUpdateOfCongruenceWithTriggerType(dmlTriggerType);
		}

		#endregion Constructors

		#region Checks

		protected virtual void CheckUpdateOfCongruenceWithTriggerType(DmlTriggerType dmlTriggerType)
		{
			if (!dmlTriggerType.HasFlag(DmlTriggerType.Update) && !dmlTriggerType.HasFlag(DmlTriggerType.All))
			{
				throw new DmlTriggerTypeException("updateOf parameter can be specified only if DmlTriggerType parameter contains DmlTriggerType.Update too, not for DmlTriggerType.Delete or DmlTriggerType.Insert only.");
			}
		}

		#endregion Checks

		#region Get infos

		protected virtual string GetTableNameFromDataAnnotation()
		{
			var attribute = typeof(DataTable).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
			return ((TableAttribute)attribute)?.Name;
		}


		protected virtual DynamicRecordChangedEventArgs GetRecordChangedEventArgs(MessagesBag messagesBag)
		{
			return new DynamicRecordChangedEventArgs(
				messagesBag,
				_server,
				_database,
				_dataBaseObjectsNamingConvention,
				this.CultureInfo);
		}

		#endregion Get infos

		#region Notifications
		protected void NotifyListenersAboutChange(Delegate[] changeSubscribedList, MessagesBag messagesBag)
		{
			if (changeSubscribedList == null) return;

			foreach (var dlg in changeSubscribedList.Where(d => d != null))
			{
				try
				{
					dlg.GetMethodInfo().Invoke(dlg.Target, new object[] { this, this.GetRecordChangedEventArgs(messagesBag) });
				}
				catch (NoMatchBetweenModelAndTableColumns)
				{
					throw;
				}
				catch (Exception ex)
				{
					this.WriteTraceMessage(TraceLevel.Error, $"Received message type = {ex.Message}.");
					// Intentionally ignored
				}
			}
		}

		#endregion Notifications

		~DynamicTableDependency()
		{
			this.Dispose(false);
		}
	}
}
