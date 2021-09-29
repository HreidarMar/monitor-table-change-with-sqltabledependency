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
using System.ComponentModel.DataAnnotations.Schema;
using System.Linq;
using System.Reflection;

using TableDependency.SqlClient.Base.Abstracts;
using TableDependency.SqlClient.Base.Delegates;
using TableDependency.SqlClient.Base.Enums;
using TableDependency.SqlClient.Base.EventArgs;
using TableDependency.SqlClient.Base.Exceptions;
using TableDependency.SqlClient.Base.Messages;
using TableDependency.SqlClient.Base.Utilities;

namespace TableDependency.SqlClient.Base
{
	public abstract class TableDependency<T> : BaseTableDependency, ITableDependency<T> where T : class, new()
	{
		#region Instance Variables

		protected IModelToTableMapper<T> _mapper;

		#endregion

		#region Events

		/// <summary>
		/// Occurs when the table content has been changed with an update, insert or delete operation.
		/// </summary>
		public abstract event ChangedEventHandler<T> OnChanged;

		#endregion

		#region Properties

		/// <summary>
		/// Gets the ModelToTableMapper.
		/// </summary>
		public IModelToTableMapper<T> Mapper => _mapper;

		#endregion

		#region Constructors

		protected TableDependency(
			string connectionString,
			string tableName = null,
			string schemaName = null,
			IModelToTableMapper<T> mapper = null,
			IUpdateOfModel<T> updateOf = null,
			ITableDependencyFilter filter = null,
			DmlTriggerType dmlTriggerType = DmlTriggerType.All,
			bool executeUserPermissionCheck = true) : base(connectionString, tableName, schemaName, filter, dmlTriggerType, executeUserPermissionCheck)
		{
			if (mapper?.Count() == 0) throw new UpdateOfException("mapper parameter is empty.");
			if (updateOf?.Count() == 0) throw new UpdateOfException("updateOf parameter is empty.");

			var tableColumnList = this.GetTableColumnsList();
			if (!tableColumnList.Any()) throw new TableWithNoColumnsException(_tableName);

			_mapper = mapper ?? ModelToTableMapperHelper<T>.GetModelMapperFromColumnDataAnnotation(tableColumnList);
			this.CheckMapperValidity(tableColumnList);

			this.CheckUpdateOfCongruenceWithTriggerType(updateOf, dmlTriggerType);
			_updateOf = this.GetUpdateOfColumnNameList(updateOf, tableColumnList);

			_userInterestedColumns = this.GetUserInterestedColumns(tableColumnList);
			if (!_userInterestedColumns.Any()) throw new NoMatchBetweenModelAndTableColumns();
			this.CheckIfUserInterestedColumnsCanBeManaged();
		}

		#endregion

		#region Checks

		protected virtual void CheckMapperValidity(IEnumerable<TableColumnInfo> tableColumnsList)
		{
			if (_mapper == null || _mapper.Count() < 1) return;

			var dbColumnNames = tableColumnsList.Select(t => t.Name.ToLowerInvariant()).ToList();

			if (_mapper.GetMappings().Select(t => t.Value).Any(mappingColumnName => !dbColumnNames.Contains(mappingColumnName.ToLowerInvariant())))
			{
				throw new ModelToTableMapperException("I cannot find any correspondence between defined ModelToTableMapper properties and database Table columns.");
			}
		}

		protected virtual void CheckUpdateOfCongruenceWithTriggerType(IUpdateOfModel<T> updateOf, DmlTriggerType dmlTriggerType)
		{
			if (updateOf == null || updateOf.Count() == 0) return;

			if (!dmlTriggerType.HasFlag(DmlTriggerType.Update) && !dmlTriggerType.HasFlag(DmlTriggerType.All))
			{
				if (updateOf.Count() > 0)
				{
					throw new DmlTriggerTypeException("updateOf parameter can be specified only if DmlTriggerType parameter contains DmlTriggerType.Update too, not for DmlTriggerType.Delete or DmlTriggerType.Insert only.");
				}
			}
		}
		#endregion

		#region Get infos

		protected virtual IEnumerable<TableColumnInfo> GetUserInterestedColumns(IEnumerable<TableColumnInfo> tableColumnsList)
		{
			var tableColumnsListFiltered = new List<TableColumnInfo>();

			foreach (var entityPropertyInfo in ModelUtil.GetModelPropertiesInfo<T>())
			{
				var notMappedAttribute = entityPropertyInfo.GetCustomAttribute(typeof(NotMappedAttribute));
				if (notMappedAttribute != null) continue;

				var propertyMappedTo = _mapper?.GetMapping(entityPropertyInfo);
				var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

				// If model property is mapped to table column keep it
				foreach (var tableColumn in tableColumnsList)
				{
					if (string.Equals(tableColumn.Name.ToLowerInvariant(), propertyName.ToLowerInvariant(), StringComparison.OrdinalIgnoreCase))
					{
						if (tableColumnsListFiltered.Any(ci => string.Equals(ci.Name, tableColumn.Name, StringComparison.OrdinalIgnoreCase)))
						{
							throw new ModelToTableMapperException("Your model specify a [Column] attributed Name that has same name of another model property.");
						}

						tableColumnsListFiltered.Add(tableColumn);
						break;
					}
				}
			}

			return tableColumnsListFiltered;
		}

		protected virtual string GetColumnNameFromModelProperty(IEnumerable<TableColumnInfo> tableColumnsList, string modelPropertyName)
		{
			var entityPropertyInfo = ModelUtil.GetModelPropertiesInfo<T>().First(mpf => mpf.Name == modelPropertyName);

			var propertyMappedTo = _mapper?.GetMapping(entityPropertyInfo);
			var propertyName = propertyMappedTo ?? entityPropertyInfo.Name;

			// If model property is mapped to table column keep it
			foreach (var tableColumn in tableColumnsList)
			{
				if (string.Equals(tableColumn.Name, propertyName, StringComparison.OrdinalIgnoreCase))
				{
					return tableColumn.Name;
				}
			}

			return modelPropertyName;
		}

		protected virtual IList<string> GetUpdateOfColumnNameList(IUpdateOfModel<T> updateOf, IEnumerable<TableColumnInfo> tableColumns)
		{
			var updateOfList = new List<string>();

			if (updateOf == null || updateOf.Count() <= 0) return updateOfList;

			foreach (var propertyInfo in updateOf.GetPropertiesInfos())
			{
				var existingMap = _mapper?.GetMapping(propertyInfo);
				if (existingMap != null)
				{
					updateOfList.Add(existingMap);
					continue;
				}

				var attribute = propertyInfo.GetCustomAttribute(typeof(ColumnAttribute));
				if (attribute != null)
				{
					var dbColumnName = ((ColumnAttribute)attribute).Name;
					if (!string.IsNullOrWhiteSpace(dbColumnName))
					{
						updateOfList.Add(dbColumnName);
						continue;
					}

					dbColumnName = GetColumnNameFromModelProperty(tableColumns, propertyInfo.Name);
					updateOfList.Add(dbColumnName);
					continue;
				}

				updateOfList.Add(propertyInfo.Name);
			}

			return updateOfList;
		}

		protected virtual string GetTableNameFromDataAnnotation()
		{
			var attribute = typeof(T).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
			return ((TableAttribute)attribute)?.Name;
		}

		protected virtual string GetSchemaNameFromDataAnnotation()
		{
			var attribute = typeof(T).GetTypeInfo().GetCustomAttribute(typeof(TableAttribute));
			return ((TableAttribute)attribute)?.Schema;
		}

		protected virtual RecordChangedEventArgs<T> GetRecordChangedEventArgs(MessagesBag messagesBag)
		{
			return new RecordChangedEventArgs<T>(
				messagesBag,
				_mapper,
				_userInterestedColumns,
				_server,
				_database,
				_dataBaseObjectsNamingConvention,
				this.CultureInfo);
		}

		#endregion

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
				catch
				{
					// Intentionally ignored
				}
			}
		}

		#endregion
	}
}