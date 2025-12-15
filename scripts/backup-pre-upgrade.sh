#!/bin/bash
# Script de Backup Pré-Upgrade Baileys v7
# Execute este script ANTES de iniciar o upgrade

set -e  # Exit on error

TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_DIR="/tmp/baileys-upgrade-backups"

echo "🔒 Iniciando backups pré-upgrade..."
mkdir -p "$BACKUP_DIR"

# 1. Git checkpoint (já feito, mas verificar)
echo "📦 Verificando git checkpoint..."
if git tag | grep -q "baileys-v6-stable"; then
  echo "✓ Tag baileys-v6-stable existe"
else
  echo "❌ Tag baileys-v6-stable não encontrada! Execute: git tag baileys-v6-stable"
  exit 1
fi

# 2. Database backup (PostgreSQL)
echo "💾 Backup do banco de dados..."
if command -v pg_dump &> /dev/null; then
  DB_HOST="${DATABASE_HOST:-localhost}"
  DB_NAME="${DATABASE_NAME:-evolution_db}"
  DB_USER="${DATABASE_USER:-postgres}"

  echo "  Database: $DB_NAME@$DB_HOST"
  pg_dump -h "$DB_HOST" -U "$DB_USER" "$DB_NAME" > "$BACKUP_DIR/database_backup_$TIMESTAMP.sql"
  echo "✓ Backup do banco salvo em: $BACKUP_DIR/database_backup_$TIMESTAMP.sql"
else
  echo "⚠️  pg_dump não encontrado - skip database backup"
fi

# 3. Redis backup (se habilitado)
echo "💾 Backup do Redis..."
if command -v redis-cli &> /dev/null && [ "$CACHE_REDIS_ENABLED" = "true" ]; then
  redis-cli --rdb "$BACKUP_DIR/redis_backup_$TIMESTAMP.rdb"
  echo "✓ Backup do Redis salvo em: $BACKUP_DIR/redis_backup_$TIMESTAMP.rdb"
else
  echo "⚠️  Redis não habilitado ou redis-cli não encontrado - skip"
fi

# 4. Docker image backup (se usando Docker)
echo "🐳 Backup da imagem Docker..."
if command -v docker &> /dev/null; then
  CURRENT_IMAGE=$(docker images --format "{{.Repository}}:{{.Tag}}" | head -n 1)
  if [ ! -z "$CURRENT_IMAGE" ]; then
    docker tag "$CURRENT_IMAGE" "${CURRENT_IMAGE}-baileys-v6-backup"
    echo "✓ Docker image tagged: ${CURRENT_IMAGE}-baileys-v6-backup"
  fi
else
  echo "⚠️  Docker não encontrado - skip"
fi

# 5. Session files backup (se existir)
echo "📁 Backup dos arquivos de sessão..."
if [ -d "instances" ]; then
  tar -czf "$BACKUP_DIR/sessions_backup_$TIMESTAMP.tar.gz" instances/
  echo "✓ Sessions backup salvo em: $BACKUP_DIR/sessions_backup_$TIMESTAMP.tar.gz"
fi

echo ""
echo "✅ Backups concluídos!"
echo "📂 Localização: $BACKUP_DIR"
echo ""
echo "Backups criados:"
ls -lh "$BACKUP_DIR"/*$TIMESTAMP*

echo ""
echo "🔐 Timestamp do backup: $TIMESTAMP"
echo "   Guarde este timestamp para usar no rollback se necessário"
