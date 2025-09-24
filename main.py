"""
Interfaz que generará un reporte a partir de una conexión de PostgreSQL 
para posteriormente formatearlo y generar un CSV en el formato deseado.
"""
import os
import sys
from typing import Dict, Any, Optional
from PyQt6 import uic, QtWidgets, QtCore
from PyQt6.QtWidgets import QMessageBox
import connection


def resource_path(relative_path: str) -> str:
    """Obtiene la ruta absoluta de un recurso, compatible con PyInstaller."""
    try:
        base_path = sys._MEIPASS  # carpeta temporal de PyInstaller
    except AttributeError:
        base_path = os.path.abspath(".")  # ejecución normal en .py
    return os.path.join(base_path, relative_path)


class Main(QtWidgets.QDialog):
    """Diálogo principal para selección de rango de fechas."""
    
    def __init__(self):
        super().__init__()
        self._setup_ui()
        self._connect_signals()
        self.open_preview = None
        
    def _setup_ui(self):
        """Configura la interfaz de usuario."""
        ui_path = os.path.join(os.path.dirname(__file__), "fechas.ui")
        uic.loadUi(resource_path(ui_path), self)
        self.setWindowTitle("Seleccionar Rango de Fechas")
        
        # Configurar fechas por defecto
        current_date = QtCore.QDate.currentDate()
        self.deFechaIni.setDate(current_date)
        self.deFechaFin.setDate(current_date)
        
        # Validaciones de fecha
        self.deFechaIni.setMaximumDate(current_date)
        self.deFechaFin.setMaximumDate(current_date)
    
    def _connect_signals(self):
        """Conecta las señales con sus respectivos slots."""
        self.accepted.connect(self.on_accept)
        self.rejected.connect(self.on_reject)
        
        # Validación automática de rango de fechas
        self.deFechaIni.dateChanged.connect(self._validate_date_range)
        self.deFechaFin.dateChanged.connect(self._validate_date_range)
    
    def _validate_date_range(self):
        """Asegura que la fecha de inicio no sea posterior a la fecha fin."""
        if self.deFechaIni.date() > self.deFechaFin.date():
            self.deFechaFin.setDate(self.deFechaIni.date())
    
    def on_accept(self):
        """Maneja la aceptación del diálogo y abre la vista previa."""
        fecha_inicio = self.deFechaIni.date().toString("yyyy-MM-dd")
        fecha_fin = self.deFechaFin.date().toString("yyyy-MM-dd")
        
        # Ocultar ventana principal
        self.hide()
        
        try:
            self.open_preview = OpenPreviewResults(fecha_inicio, fecha_fin, self)
            # Conectar el evento de cierre para manejar correctamente la aplicación
            self.open_preview.finished.connect(self._handle_preview_finished)
            self.open_preview.exec()
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al abrir vista previa: {str(e)}")
            self.show()
    
    def _handle_preview_finished(self, result):
        """Maneja el cierre de la ventana de vista previa."""
        if result == QtWidgets.QDialog.DialogCode.Accepted:
            # Si se exportó el archivo, cerrar aplicación completamente
            QtWidgets.QApplication.quit()
        else:
            # Si se canceló, mostrar ventana principal de nuevo
            self.show()
    
    def on_reject(self):
        """Maneja el rechazo del diálogo cerrando la aplicación."""
        QtWidgets.QApplication.quit()
    
    def closeEvent(self, event):
        """Maneja el evento de cierre de ventana."""
        QtWidgets.QApplication.quit()
        event.accept()


class OpenPreviewResults(QtWidgets.QDialog):
    """Diálogo para mostrar vista previa de resultados y exportar CSV."""
    
    # Constantes de clase
    RESULTADOS_ALIAS = {
        852: "TSH", 859: "IRT", 854: "PKU", 883: "17OH",
        886: "JarabeA1", 885: "JarabeA2", 888: "Tyr",
        889: "HbF", 890: "HbA", 891: "HbS", 892: "HbC"
    }
    
    COLUMNAS_NORMALES = [
        "codigoE", "Boleta", "FechaTomaMx", "Paciente", "Edad", "Sexo", "Expediente",
        "Recepcion", "Procesamiento", "FResultado", "FechaRechazo", "EstadoPaciente",
        "StdoBoleta", "Update", "ReferidoPor", "Id"
    ]
    
    def __init__(self, fecha_inicio: str, fecha_fin: str, parent: Optional[QtWidgets.QWidget] = None):
        super().__init__(parent)
        self.fecha_inicio = fecha_inicio
        self.fecha_fin = fecha_fin
        self.data: Dict[str, Any] = {}
        
        self._setup_ui()
        self._connect_signals()
        self._load_data()
    
    def _setup_ui(self):
        """Configura la interfaz de usuario."""
        uic.loadUi(resource_path("preview.ui"), self)
        self.setWindowTitle(f"Vista Previa - {self.fecha_inicio} a {self.fecha_fin}")
        
        # Configurar tabla
        columnas_tabla = self.COLUMNAS_NORMALES + list(self.RESULTADOS_ALIAS.values())
        self.tblResults.setColumnCount(len(columnas_tabla))
        self.tblResults.setHorizontalHeaderLabels(columnas_tabla)
        self.tblResults.setAlternatingRowColors(True)
        self.tblResults.setSortingEnabled(True)
        
        # Inicialmente deshabilitar exportación hasta cargar datos
        self.btnExport.setEnabled(False)
    
    def _connect_signals(self):
        """Conecta las señales con sus respectivos slots."""
        self.btnback.clicked.connect(self._on_back)
        self.btnExport.clicked.connect(self._on_export)
    
    def _load_data(self):
        """Carga los datos del reporte."""
        try:
            # Mostrar cursor de espera
            QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CursorShape.WaitCursor)
            
            self.data = self._generate_report()
            if self.data:
                self._populate_table()
                self.btnExport.setEnabled(True)
            else:
                QMessageBox.information(self, "Sin datos", "No se encontraron datos para el rango seleccionado.")
                
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error al cargar datos: {str(e)}")
        finally:
            # Restaurar cursor normal
            QtWidgets.QApplication.restoreOverrideCursor()
    
    def _generate_report(self) -> Dict[str, Any]:
        """Genera el reporte conectándose a la base de datos."""
        conn = connection.connect_to_db()
        if conn is None:
            raise ConnectionError("No se pudo conectar a la base de datos")
        
        return connection.generate_report(conn, self.fecha_inicio, self.fecha_fin)
    
    def _populate_table(self):
        """Llena la tabla con los datos del reporte."""
        if not self.data:
            return
        
        # Deshabilitar ordenamiento durante la carga para mejor rendimiento
        self.tblResults.setSortingEnabled(False)
        self.tblResults.setRowCount(len(self.data))
        
        for row_idx, boleta in enumerate(self.data.values()):
            # Llenar columnas normales
            for col_idx, col_name in enumerate(self.COLUMNAS_NORMALES):
                value = boleta.get(col_name, "#NULL#")
                item = QtWidgets.QTableWidgetItem(str(value))
                self.tblResults.setItem(row_idx, col_idx, item)
            
            # Llenar columnas de resultados usando aliases
            for col_idx, (id_resultado, _) in enumerate(self.RESULTADOS_ALIAS.items(), 
                                                       start=len(self.COLUMNAS_NORMALES)):
                value = boleta.get("Resultados", {}).get(id_resultado, "#NULL#")
                item = QtWidgets.QTableWidgetItem(str(value))
                self.tblResults.setItem(row_idx, col_idx, item)
        
        # Reactivar ordenamiento y ajustar columnas
        self.tblResults.setSortingEnabled(True)
        self.tblResults.resizeColumnsToContents()
        
        # Mostrar estadísticas básicas
        self._show_statistics()
    
    def _show_statistics(self):
        """Muestra estadísticas básicas en el título de la ventana."""
        total = len(self.data)
        con_resultados = sum(1 for b in self.data.values() if b.get("StdoBoleta") == "A")
        self.setWindowTitle(
            f"Vista Previa - {self.fecha_inicio} a {self.fecha_fin} "
            f"(Total: {total}, Con resultados: {con_resultados})"
        )
    
    def _on_back(self):
        """Maneja el botón de regreso."""
        self.reject()  # Esto cerrará el diálogo con código de rechazo
    
    def _on_export(self):
        """Maneja la exportación de datos a CSV."""
        if not self.data:
            QMessageBox.warning(self, "Sin datos", "No hay datos para exportar.")
            return
        
        try:
            # Obtener nombre de archivo sugerido
            filename = f"reporte_labsis_{self.fecha_inicio}_a_{self.fecha_fin}.csv"
            
            # Abrir diálogo para guardar archivo
            filepath, _ = QtWidgets.QFileDialog.getSaveFileName(
                self,
                "Guardar reporte CSV",
                filename,
                "Archivos CSV (*.csv);;Todos los archivos (*)"
            )
            
            if filepath:
                # Exportar usando la función del módulo connection
                connection.write_to_csv(self.data, filepath)
                QMessageBox.information(
                    self, 
                    "Exportación exitosa", 
                    f"El reporte se ha exportado correctamente a:\n{filepath}"
                )
                # Cerrar con código de aceptación para indicar exportación exitosa
                self.accept()
            
        except Exception as e:
            QMessageBox.critical(self, "Error de exportación", f"Error al exportar: {str(e)}")
    
    def closeEvent(self, event):
        """Maneja el evento de cierre de ventana."""
        self.reject()
        event.accept()


if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)
    
    # Configurar estilo de aplicación (opcional)
    app.setStyle("Fusion")
    
    try:
        window = Main()
        window.show()
        sys.exit(app.exec())
    except Exception as e:
        QMessageBox.critical(None, "Error Fatal", f"Error al inicializar la aplicación: {str(e)}")
        sys.exit(1)