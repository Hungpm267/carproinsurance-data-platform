-- ============================================================================
-- Helper: title case (no INITCAP in Fabric DW)
-- ============================================================================
CREATE   FUNCTION silver.fn_title_case(@input VARCHAR(4000))
RETURNS VARCHAR(4000)
AS
BEGIN
    IF @input IS NULL RETURN NULL;
    DECLARE @result VARCHAR(4000) = LOWER(@input);
    DECLARE @i INT = 1, @len INT = LEN(@result);
    WHILE @i <= @len
    BEGIN
        IF @i = 1 OR SUBSTRING(@result, @i - 1, 1) = ' '
            SET @result = STUFF(@result, @i, 1, UPPER(SUBSTRING(@result, @i, 1)));
        SET @i += 1;
    END
    RETURN @result;
END;