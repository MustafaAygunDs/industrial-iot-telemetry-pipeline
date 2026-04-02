DROP TABLE IF EXISTS hiz_kayitlari CASCADE;

CREATE TABLE hiz_kayitlari (
    id SERIAL PRIMARY KEY,
    arac_id VARCHAR(50) NOT NULL,
    hiz INTEGER NOT NULL,
    motor_sicakligi INTEGER,
    timestamp TIMESTAMP NOT NULL,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_hiz_kayitlari_arac_id ON hiz_kayitlari(arac_id);
CREATE INDEX idx_hiz_kayitlari_timestamp ON hiz_kayitlari(timestamp DESC);
CREATE INDEX idx_hiz_kayitlari_hiz ON hiz_kayitlari(hiz) WHERE hiz > 120;

INSERT INTO hiz_kayitlari (arac_id, hiz, motor_sicakligi, timestamp)
VALUES 
    ('Corolla-01', 95, 90, CURRENT_TIMESTAMP),
    ('Corolla-02', 125, 95, CURRENT_TIMESTAMP),
    ('Corolla-03', 150, 102, CURRENT_TIMESTAMP)
ON CONFLICT DO NOTHING;

SELECT 'Hazır!' as durum, COUNT(*) as kayit_sayisi FROM hiz_kayitlari;
